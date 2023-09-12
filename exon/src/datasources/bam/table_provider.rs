use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    common::FileCompressionType,
    datasource::{
        listing::{ListingTableConfig, ListingTableUrl},
        physical_plan::FileScanConfig,
        TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionState,
    logical_expr::{TableProviderFilterPushDown, TableType},
    physical_plan::{empty::EmptyExec, ExecutionPlan, Statistics},
    prelude::Expr,
};

use crate::{datasources::ExonFileType, io::exon_object_store};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingBAMTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingBAMTableOptions>,
}

impl ListingBAMTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingBAMTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

use super::{array_builder::schema, BAMScan};

#[derive(Debug, Clone)]
/// Listing options for a BAM table
pub struct ListingBAMTableOptions {
    file_extension: String,
}

impl ListingBAMTableOptions {
    /// Create a new set of options
    pub fn new() -> Self {
        let file_extension =
            ExonFileType::BAM.get_file_extension(FileCompressionType::UNCOMPRESSED);

        Self { file_extension }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();

        Ok(Arc::new(schema))
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = BAMScan::new(conf);

        // if let Some(region_filter) = &self.region_filter {
        //     scan = scan.with_region_filter(region_filter.clone());
        // }

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BAM listing table
pub struct ListingBAMTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingBAMTableOptions,
}

impl ListingBAMTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingBAMTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
        Ok(Self {
            table_paths: config.inner.table_paths,
            table_schema,
            options: config
                .options
                .ok_or_else(|| DataFusionError::Internal(String::from("Options must be set")))?,
        })
    }
}

#[async_trait]
impl TableProvider for ListingBAMTable {
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
            .map(|_f| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let object_store_url = if let Some(url) = self.table_paths.get(0) {
            url.object_store()
        } else {
            return Ok(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))));
        };

        let object_store = state.runtime_env().object_store(object_store_url.clone())?;

        let partitioned_file_lists = vec![
            exon_object_store::list_files_for_scan(
                object_store,
                self.table_paths.clone(),
                &self.options.file_extension,
            )
            .await?,
        ];

        let file_scan_config = FileScanConfig {
            object_store_url: object_store_url,
            file_schema: Arc::clone(&self.table_schema), // Actually should be file schema??
            file_groups: partitioned_file_lists,
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            output_ordering: Vec::new(),
            table_partition_cols: Vec::new(),
            infinite_source: false,
        };

        let plan = self.options.create_physical_plan(file_scan_config).await?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {

    use datafusion::{
        common::FileCompressionType, datasource::listing::ListingTableUrl, prelude::SessionContext,
    };

    use crate::datasources::{ExonFileType, ExonListingTableFactory};

    #[tokio::test]
    async fn test_read_bam() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data").unwrap();

        let exon_listing_table_factory = ExonListingTableFactory::new();

        let table = exon_listing_table_factory
            .create_from_file_type(
                &session_state,
                ExonFileType::BAM,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
            )
            .await
            .unwrap();

        let df = ctx.read_table(table).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 61)
    }

    // #[tokio::test]
    // async fn test_read_with_index() {
    //     let ctx = SessionContext::new();
    //     let session_state = ctx.state();

    //     let table_path = ListingTableUrl::parse("test-data").unwrap();

    //     let region: Region = "chr1:1-12209153".parse().unwrap();
    //     let fasta_format = Arc::new(BAMFormat::default().with_region_filter(region));

    //     let lo = ListingOptions::new(fasta_format.clone()).with_file_extension("bam");

    //     let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

    //     let config = ListingTableConfig::new(table_path)
    //         .with_listing_options(lo)
    //         .with_schema(resolved_schema);

    //     let provider = Arc::new(ListingTable::try_new(config).unwrap());
    //     let df = ctx.read_table(provider.clone()).unwrap();

    //     let mut row_cnt = 0;
    //     let bs = df.collect().await.unwrap();
    //     for batch in bs {
    //         row_cnt += batch.num_rows();
    //     }
    //     assert_eq!(row_cnt, 55)
    // }
}
