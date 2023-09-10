use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode},
        FileCompressionType, ToDFSchema,
    },
    datasource::{
        file_format::FileFormat,
        listing::{FileRange, ListingTableConfig, ListingTableUrl, PartitionedFile},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_plan::{empty::EmptyExec, project_schema, ExecutionPlan, PhysicalExpr, Statistics},
    prelude::Expr,
};
use futures::TryStreamExt;
use noodles::core::Region;
use object_store::ObjectMeta;

use crate::{
    datasources::ExonFileType,
    physical_optimizer::merging::{try_merge_chrom_exprs, try_merge_region_with_interval},
    physical_plan::{
        chrom_physical_expr::ChromPhysicalExpr, interval_physical_expr::IntervalPhysicalExpr,
        region_physical_expr::RegionPhysicalExpr,
    },
};

use super::file_format::get_byte_range_for_file;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct VCFListingTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingVCFTableOptions>,
}

impl VCFListingTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingVCFTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Options specific to the VCF file format
pub struct ListingVCFTableOptions {
    /// The file format
    format: Arc<dyn FileFormat>,

    /// The extension of the files to read
    file_extension: String,
}

impl ListingVCFTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let format = ExonFileType::VCF.get_file_format(file_compression_type);
        let file_extension = ExonFileType::VCF.get_file_extension(file_compression_type);

        Self {
            format,
            file_extension,
        }
    }

    /// Infer the schema of the files in the table
    pub async fn infer_schema<'a>(
        &'a self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let mut files: Vec<ObjectMeta> = Vec::new();

        if table_path.to_string().ends_with('/') {
            let store_list = store.list(Some(table_path.prefix())).await?;
            store_list
                .try_for_each(|v| {
                    let path = v.location.clone();
                    let extension_match = path.as_ref().ends_with(self.file_extension.as_str());
                    let glob_match = table_path.contains(&path);
                    if extension_match && glob_match {
                        files.push(v);
                    }
                    futures::future::ready(Ok(()))
                })
                .await?;

            self.format.infer_schema(state, &store, &files).await
        } else {
            let store_head = match store.head(table_path.prefix()).await {
                Ok(object_meta) => object_meta,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Unable to get path info: {}",
                        e
                    )))
                }
            };

            self.format.infer_schema(state, &store, &[store_head]).await
        }
    }
}

#[derive(Debug, Clone)]
/// A VCF listing table
pub struct ListingVCFTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingVCFTableOptions,
}

impl ListingVCFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: VCFListingTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }

    /// List the files that need to be read for a given set of regions
    pub async fn list_files_for_scan(
        &self,
        state: &SessionState,
        regions: Vec<Region>,
    ) -> Result<Vec<Vec<PartitionedFile>>> {
        let store = if let Some(url) = self.table_paths.get(0) {
            state.runtime_env().object_store(url)?
        } else {
            return Ok(vec![]);
        };

        let mut lists = Vec::new();

        let file_extension = self.options.file_extension.as_str();

        for table_path in &self.table_paths {
            if table_path.as_str().ends_with('/') {
                let store_list = store.list(Some(table_path.prefix())).await?;

                // iterate over all files in the listing
                let mut result_vec: Vec<PartitionedFile> = vec![];

                store_list
                    .try_for_each(|v| {
                        let path = v.location.clone();
                        let extension_match = path.as_ref().ends_with(file_extension);
                        let glob_match = table_path.contains(&path);
                        if extension_match && glob_match {
                            result_vec.push(v.into());
                        }
                        futures::future::ready(Ok(()))
                    })
                    .await?;

                lists.push(result_vec);
            } else {
                let store_head = match store.head(table_path.prefix()).await {
                    Ok(object_meta) => object_meta,
                    Err(e) => {
                        return Err(DataFusionError::Execution(format!(
                            "Unable to get path info: {}",
                            e
                        )))
                    }
                };

                lists.push(vec![store_head.into()]);
            }
        }

        let mut new_list = vec![];
        for partition_files in lists {
            let mut new_partition_files = vec![];

            for partition_file in partition_files {
                for region in regions.iter() {
                    let byte_ranges = match get_byte_range_for_file(
                        store.clone(),
                        &partition_file.object_meta,
                        region,
                    )
                    .await
                    {
                        Ok(byte_ranges) => byte_ranges,
                        Err(_) => {
                            continue;
                        }
                    };

                    for byte_range in byte_ranges {
                        let mut new_partition_file = partition_file.clone();

                        new_partition_file.range = Some(FileRange {
                            start: byte_range.start().compressed() as i64,
                            end: byte_range.end().compressed() as i64,
                        });

                        new_partition_files.push(new_partition_file);
                    }
                }
            }

            new_list.push(new_partition_files);
        }

        Ok(new_list)
    }
}

fn transform(e: Arc<dyn PhysicalExpr>) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    match e
        .as_any()
        .downcast_ref::<datafusion::physical_plan::expressions::BinaryExpr>()
    {
        Some(be) => {
            if let Ok(chrom_expr) = ChromPhysicalExpr::try_from(be.clone()) {
                let region_expr = RegionPhysicalExpr::new(Arc::new(chrom_expr), None);

                return Ok(Transformed::Yes(Arc::new(region_expr)));
            }

            if let Ok(interval_expr) = IntervalPhysicalExpr::try_from(be.clone()) {
                return Ok(Transformed::Yes(Arc::new(interval_expr)));
            }

            // Now we need to check if the left and right side can be merged in a single expression.

            // Case 1: left and right are both chrom expressions, and need to be downcast to chrom expressions
            if let Some(left_chrom) = be.left().as_any().downcast_ref::<ChromPhysicalExpr>() {
                if let Some(right_chrom) = be.right().as_any().downcast_ref::<ChromPhysicalExpr>() {
                    match try_merge_chrom_exprs(left_chrom, right_chrom) {
                        Ok(Some(new_expr)) => return Ok(Transformed::Yes(Arc::new(new_expr))),
                        Ok(None) => return Ok(Transformed::No(e)),
                        Err(e) => return Err(e),
                    }
                }
            }

            // Case 2: left is a chrom expression and right is an interval expression
            if let Some(_left_chrom) = be.left().as_any().downcast_ref::<ChromPhysicalExpr>() {
                if let Some(_right_interval) =
                    be.right().as_any().downcast_ref::<IntervalPhysicalExpr>()
                {
                    let new_expr =
                        RegionPhysicalExpr::new(be.left().clone(), Some(be.right().clone()));

                    return Ok(Transformed::Yes(Arc::new(new_expr)));
                }
            }

            // Case 3: left is a region expression and the right is an interval expression
            if let Some(left_region) = be.left().as_any().downcast_ref::<RegionPhysicalExpr>() {
                if let Some(right_interval) =
                    be.right().as_any().downcast_ref::<IntervalPhysicalExpr>()
                {
                    let new_region = try_merge_region_with_interval(left_region, right_interval)?;

                    if let Some(new_region) = new_region {
                        return Ok(Transformed::Yes(Arc::new(new_region)));
                    }
                }
            }

            Ok(Transformed::No(e))
        }
        None => Ok(Transformed::No(e)),
    }
}

// F: Fn(Self) -> Result<Transformed<Self>>,

#[async_trait]
impl TableProvider for ListingVCFTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_f| TableProviderFilterPushDown::Inexact)
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                &self.table_schema,
                state.execution_props(),
            )?;

            // TODO: transform the filters to push down partition filters
            let new_filters = filters.transform(&transform)?;

            Some(new_filters)
        } else {
            None
        };

        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        if filters.clone().is_none() {
            todo!("Implement no filter case for VCF scan");
        }

        let region_exprr = filters.clone().unwrap();
        let new_region_expr = region_exprr.as_any().downcast_ref::<RegionPhysicalExpr>();

        // if we got a filter check if it is a RegionFilter
        if let Some(region_expr) = new_region_expr {
            let regions = vec![region_expr.region()?];

            let partitioned_file_lists = self.list_files_for_scan(state, regions).await?;

            // if no files need to be read, return an `EmptyExec`
            if partitioned_file_lists.is_empty() {
                let schema = self.schema();
                let projected_schema = project_schema(&schema, projection)?;
                return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
            }

            let f = filters.unwrap().clone();

            return self
                .options
                .format
                .create_physical_plan(
                    state,
                    FileScanConfig {
                        object_store_url,
                        file_schema: Arc::clone(&self.table_schema), // Actually should be file schema??
                        file_groups: partitioned_file_lists,
                        statistics: Statistics::default(),
                        projection: projection.cloned(),
                        limit,
                        output_ordering: Vec::new(),
                        table_partition_cols: Vec::new(),
                        infinite_source: false,
                    },
                    Some(&f),
                )
                .await;
        }

        todo!("Implement non-region filter case for VCF scan");
    }
}
