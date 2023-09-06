use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::{FileCompressionType, ToDFSchema},
    datasource::{
        file_format::FileFormat,
        listing::{ListingTableConfig, ListingTableUrl, PartitionedFile},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    optimizer::utils::conjunction,
    physical_expr::create_physical_expr,
    physical_plan::{empty::EmptyExec, project_schema, ExecutionPlan, Statistics},
    prelude::Expr,
};
use futures::TryStreamExt;
use object_store::ObjectMeta;

use crate::datasources::ExonFileType;

#[derive(Debug, Clone)]
pub struct VCFListingTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingVCFTableOptions>,
}

impl VCFListingTableConfig {
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

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
pub struct ListingVCFTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingVCFTableOptions,
}

impl ListingVCFTable {
    pub fn try_new(config: VCFListingTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }

    pub async fn list_files_for_scan<'a>(
        &'a self,
        state: &'a SessionState,
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
                let mut result_vec = vec![];

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

        Ok(lists)
    }
}

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
        // TODO: Implement this
        Ok(filters
            .iter()
            .map(|_f| TableProviderFilterPushDown::Exact)
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitioned_file_lists = self.list_files_for_scan(state).await?;

        // if no files need to be read, return an `EmptyExec`
        if partitioned_file_lists.is_empty() {
            let schema = self.schema();
            let projected_schema = project_schema(&schema, projection)?;
            return Ok(Arc::new(EmptyExec::new(false, projected_schema)));
        }

        let filters = if let Some(expr) = conjunction(filters.to_vec()) {
            // NOTE: Use the table schema (NOT file schema) here because `expr` may contain references to partition columns.
            let table_df_schema = self.table_schema.as_ref().clone().to_dfschema()?;
            let filters = create_physical_expr(
                &expr,
                &table_df_schema,
                &self.table_schema,
                state.execution_props(),
            )?;
            Some(filters)
        } else {
            None
        };

        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        // Do we want to downcast FileFormat and set the region filter here?

        // create the execution plan
        self.options
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
                filters.as_ref(),
            )
            .await
    }
}
