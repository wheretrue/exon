use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::{
        tree_node::{Transformed, TreeNode},
        FileCompressionType, ToDFSchema,
    },
    datasource::{
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
use noodles::{bgzf, core::Region, csi::index::reference_sequence::bin::Chunk, vcf};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use crate::{
    datasources::ExonFileType,
    physical_optimizer::merging::{try_merge_chrom_exprs, try_merge_region_with_interval},
    physical_plan::{
        chrom_physical_expr::ChromPhysicalExpr, interval_physical_expr::IntervalPhysicalExpr,
        region_physical_expr::RegionPhysicalExpr,
    },
};

use super::{VCFScan, VCFSchemaBuilder};

/// For a given file, get the list of byte ranges that contain the data for the given region.
pub async fn get_byte_range_for_file(
    object_store: Arc<dyn ObjectStore>,
    object_meta: &ObjectMeta,
    region: &Region,
) -> std::io::Result<Vec<Chunk>> {
    let tbi_path = object_meta.location.clone().to_string() + ".tbi";
    let tbi_path = Path::from(tbi_path);

    let index_bytes = object_store.get(&tbi_path).await?.bytes().await?;

    let cursor = std::io::Cursor::new(index_bytes);
    let index = noodles::tabix::Reader::new(cursor).read_index()?;

    let id = resolve_region(&index, region)?;
    let chunks = index.query(id, region.interval())?;

    Ok(chunks)
}

/// Given a region, use its name to resolve the reference sequence index.
fn resolve_region(index: &noodles::csi::Index, region: &Region) -> std::io::Result<usize> {
    let header = index.header().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing tabix header")
    })?;

    let i = header
        .reference_sequence_names()
        .get_index_of(region.name())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "region reference sequence does not exist in reference sequences: {region:?}"
                ),
            )
        })?;

    Ok(i)
}

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
    /// The extension of the files to read
    file_extension: String,

    /// The region to filter on
    region: Option<Region>,

    /// The file compression type
    file_compression_type: FileCompressionType,
}

impl ListingVCFTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_compression_type = file_compression_type;
        let file_extension = ExonFileType::VCF.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
            region: None,
        }
    }

    /// Set the region for the table options. This is used to filter the records
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    async fn infer_schema_from_object_meta(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let get_result = store.get(&objects[0].location).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let exon_settings = state
            .config()
            .get_extension::<crate::config::ExonConfigExtension>();

        let vcf_parse_info = exon_settings
            .as_ref()
            .map(|s| s.vcf_parse_info)
            .unwrap_or(false);

        let vcf_parse_format = exon_settings
            .as_ref()
            .map(|s| s.vcf_parse_format)
            .unwrap_or(false);

        let mut schema_builder = match self.file_compression_type {
            FileCompressionType::GZIP => {
                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                let mut vcf_reader = vcf::AsyncReader::new(bgzf_reader);

                let header = vcf_reader.read_header().await?;

                VCFSchemaBuilder::default()
                    .with_header(header)
                    .with_parse_info(vcf_parse_info)
                    .with_parse_formats(vcf_parse_format)
            }
            FileCompressionType::UNCOMPRESSED => {
                let mut vcf_reader = vcf::AsyncReader::new(stream_reader);

                let header = vcf_reader.read_header().await?;

                VCFSchemaBuilder::default()
                    .with_header(header)
                    .with_parse_info(vcf_parse_info)
                    .with_parse_formats(vcf_parse_format)
            }
            _ => {
                return Err(DataFusionError::Execution(
                    "Unsupported file compression type".to_string(),
                ))
            }
        };

        let schema = schema_builder.build()?;

        Ok(Arc::new(schema))
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

            self.infer_schema_from_object_meta(state, &store, &files)
                .await
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

            self.infer_schema_from_object_meta(state, &store, &[store_head])
                .await
        }
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // if we got filters, we need to check if they are region filters
        if let Some(filters) = filters {
            // Downcast the filters to a Region filter if possible.
            if let Some(region_filter) = filters.as_any().downcast_ref::<RegionPhysicalExpr>() {
                let region = region_filter.region()?;

                let scan = VCFScan::new(conf, self.file_compression_type)?.with_filter(region);

                return Ok(Arc::new(scan));
            }
        }

        if let Some(region) = &self.region {
            let scan = VCFScan::new(conf, self.file_compression_type)?.with_filter(region.clone());

            return Ok(Arc::new(scan));
        }

        let scan = VCFScan::new(conf, self.file_compression_type)?;

        Ok(Arc::new(scan))
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

                        let start = u64::from(byte_range.start());
                        let end = u64::from(byte_range.end());

                        new_partition_file.range = Some(FileRange {
                            start: start as i64,
                            end: end as i64,
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

            let new_filters = filters.transform(&transform)?;

            Some(new_filters)
        } else {
            None
        };

        tracing::debug!("Filters for VCF scan: {:?}", filters);

        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        if filters.clone().is_none() {
            let object_store = state.runtime_env().object_store(object_store_url.clone())?;

            let partitioned_file_lists = vec![
                crate::physical_plan::object_store::list_files_for_scan(
                    object_store,
                    self.table_paths.clone(),
                    &self.options.file_extension,
                )
                .await?,
            ];

            let file_scan_config = FileScanConfig {
                object_store_url,
                file_schema: Arc::clone(&self.table_schema), // Actually should be file schema??
                file_groups: partitioned_file_lists,
                statistics: Statistics::default(),
                projection: projection.cloned(),
                limit,
                output_ordering: Vec::new(),
                table_partition_cols: Vec::new(),
                infinite_source: false,
            };

            let table = self
                .options
                .create_physical_plan(file_scan_config, None)
                .await?;

            return Ok(table);
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

            let file_scan_config = FileScanConfig {
                object_store_url,
                file_schema: Arc::clone(&self.table_schema), // Actually should be file schema??
                file_groups: partitioned_file_lists,
                statistics: Statistics::default(),
                projection: projection.cloned(),
                limit,
                output_ordering: Vec::new(),
                table_partition_cols: Vec::new(),
                infinite_source: false,
            };

            let table = self
                .options
                .create_physical_plan(file_scan_config, Some(&f))
                .await?;

            return Ok(table);
        }

        todo!("Implement non-region filter case for VCF scan");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        datasources::{
            vcf::{table_provider::get_byte_range_for_file, VCFScan},
            ExonListingTableFactory,
        },
        tests::{test_listing_table_dir, test_listing_table_url, test_path},
        ExonSessionExt,
    };

    use arrow::datatypes::DataType;
    use datafusion::{
        physical_plan::{coalesce_partitions::CoalescePartitionsExec, filter::FilterExec},
        prelude::SessionContext,
    };
    use noodles::bgzf::VirtualPosition;
    use object_store::{local::LocalFileSystem, ObjectStore};

    #[tokio::test]
    async fn test_byte_range_calculation() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_listing_table_dir("bigger-index", "test.vcf.gz");
        let object_store = Arc::new(LocalFileSystem::new());

        let object_meta = object_store.head(&path).await?;

        let region = "chr1:1-3388930".parse()?;

        let chunks = get_byte_range_for_file(object_store, &object_meta, &region).await?;

        assert_eq!(chunks.len(), 1);

        let chunk = chunks[0];
        assert_eq!(chunk.start(), VirtualPosition::from(621346816));
        assert_eq!(chunk.end(), VirtualPosition::from(3014113427456));

        Ok(())
    }

    #[tokio::test]
    async fn test_region_pushdown() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let table_path = test_path("vcf", "index.vcf.gz");
        let table_path = table_path.to_str().unwrap();

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let sql_statements = vec![
            "SELECT * FROM vcf_file WHERE chrom = '1' AND pos = 100000;",
            "SELECT * FROM vcf_file WHERE chrom = '1' AND pos BETWEEN 100000 AND 2000000;",
            "SELECT * FROM vcf_file WHERE chrom = '1'",
        ];

        for sql_statement in sql_statements {
            let df = ctx.sql(sql_statement).await?;

            let physical_plan = ctx.state().create_physical_plan(df.logical_plan()).await?;

            if let Some(scan) = physical_plan.as_any().downcast_ref::<FilterExec>() {
                // The file scan is wrapped in a CoalescePartitionsExec
                let scan = scan
                    .input()
                    .as_any()
                    .downcast_ref::<CoalescePartitionsExec>()
                    .unwrap();

                // Check the input is a VCF scan...
                if let Some(scan) = scan.input().as_any().downcast_ref::<VCFScan>() {
                    // ... and that it has a region filter.
                    assert!(scan.region_filter().is_some());
                } else {
                    panic!(
                        "expected VCFScan for {} in {:#?}",
                        sql_statement, physical_plan
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_vcf_parsing_string() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("vcf", "index.vcf");

        let sql = "SET exon.vcf_parse_info = true;";
        ctx.sql(sql).await?;

        let sql = "SET exon.vcf_parse_format = true;";
        ctx.sql(sql).await?;

        let sql = format!(
            "CREATE EXTERNAL TABLE vcf_file STORED AS VCF LOCATION '{}';",
            table_path.to_str().unwrap(),
        );
        ctx.sql(&sql).await?;

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1' AND pos = 100000;";
        let df = ctx.sql(sql).await?;

        // Check that the last two columns are strings.
        let schema = df.schema();

        assert_eq!(schema.field(7).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(8).data_type(), &DataType::Utf8);

        Ok(())
    }

    #[tokio::test]
    async fn test_uncompressed_read() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let table_path = test_path("vcf", "index.vcf");
        let table_path = table_path.to_str().unwrap();

        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &ctx.state(),
                crate::datasources::ExonFileType::VCF,
                datafusion::common::FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
            )
            .await?;

        ctx.register_table("vcf_file", table)?;

        let df = ctx
            .sql("SELECT chrom, pos, id FROM vcf_file")
            .await
            .unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 621);

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_files() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let url = test_listing_table_url("two-vcf");

        ctx.register_vcf_file("vcf_file", url.to_string().as_str())
            .await?;

        let sql = "SELECT * FROM vcf_file WHERE chrom = '1'";
        let df = ctx.sql(sql).await?;

        let plan = df.create_physical_plan().await?;

        // Check we can downcast to a FilterExec with a VCFScan input.
        if let Some(scan) = plan.as_any().downcast_ref::<FilterExec>() {
            let scan = scan
                .input()
                .as_any()
                .downcast_ref::<CoalescePartitionsExec>()
                .unwrap();

            if let Some(scan) = scan.input().as_any().downcast_ref::<VCFScan>() {
                // We should have two file groups with one file not one with two files.
                assert_eq!(scan.base_config().file_groups.len(), 2);
            } else {
                panic!("expected VCFScan for {}", sql);
            }
        } else {
            panic!("expected FilterExec for {}", sql);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_with_biobear_file() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();

        let table_path = test_path("biobear-vcf", "vcf_file.vcf.gz");
        let table_path = table_path.to_str().unwrap();

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let sql = "SELECT * FROM vcf_file";
        let df = ctx.sql(sql).await?;

        let batches = df.collect().await?;
        for batch in batches {
            assert!(batch.num_rows() > 0);

            // Check the schema is of the correct size.
            assert_eq!(batch.schema().fields().len(), 9);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_compressed_read_with_region() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new_exon();
        let table_path = test_path("bigger-index", "test.vcf.gz");
        let table_path = table_path.to_str().unwrap();

        ctx.register_vcf_file("vcf_file", table_path).await?;

        let df = ctx
            .sql("SELECT chrom, pos FROM vcf_file WHERE chrom = 'chr1' AND pos BETWEEN 3388920 AND 3388930")
            .await?;

        let mut row_cnt = 0;
        let bs = df.collect().await?;
        for batch in bs {
            row_cnt += batch.num_rows();

            assert_eq!(batch.schema().field(0).name(), "chrom");
            assert_eq!(batch.schema().field(1).name(), "pos");

            assert_eq!(batch.schema().fields().len(), 2);
        }

        assert_eq!(row_cnt, 1);

        Ok(())
    }
}
