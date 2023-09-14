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
    physical_plan::{empty::EmptyExec, ExecutionPlan},
    prelude::Expr,
};
use futures::TryStreamExt;
use noodles::{bcf, core::Region};
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

use crate::{
    datasources::{vcf::VCFSchemaBuilder, ExonFileType},
    physical_plan::file_scan_config_builder,
};

use super::BCFScan;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingBCFTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingBCFTableOptions>,
}

impl ListingBCFTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingBCFTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a BCF table
pub struct ListingBCFTableOptions {
    file_extension: String,

    region: Option<Region>,
}

impl Default for ListingBCFTableOptions {
    fn default() -> Self {
        Self {
            file_extension: ExonFileType::BCF.get_file_extension(FileCompressionType::UNCOMPRESSED),
            region: None,
        }
    }
}

impl ListingBCFTableOptions {
    /// Set the region for the table options. This is used to filter the records
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    /// Infer the schema for the table
    pub async fn infer_schema<'a>(
        &self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> datafusion::error::Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let get_result = if table_path.to_string().ends_with('/') {
            let list = store.list(Some(table_path.prefix())).await?;
            let collected_list = list.try_collect::<Vec<_>>().await?;
            let first = collected_list
                .first()
                .ok_or_else(|| DataFusionError::Execution("No files found".to_string()))?;

            store.get(&first.location).await?
        } else {
            store.get(table_path.prefix()).await?
        };

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let mut bcf_reader = bcf::AsyncReader::new(stream_reader);
        bcf_reader.read_file_format().await?;

        let header_str = bcf_reader.read_header().await?;
        let header = header_str
            .parse::<noodles::vcf::Header>()
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let mut schema_builder = VCFSchemaBuilder::default()
            .with_header(header)
            .with_parse_formats(true)
            .with_parse_info(true);

        let schema = schema_builder.build()?;

        Ok(Arc::new(schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut scan = BCFScan::new(conf.clone());

        if let Some(region_filter) = &self.region {
            scan = scan.with_region_filter(region_filter.clone());
        }

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A BCF listing table
pub struct ListingBCFTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingBCFTableOptions,
}

impl ListingBCFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingBCFTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
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
impl TableProvider for ListingBCFTable {
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
            crate::physical_plan::object_store::list_files_for_scan(
                object_store,
                self.table_paths.clone(),
                &self.options.file_extension,
            )
            .await?,
        ];

        let file_scan_config = file_scan_config_builder::FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&self.table_schema),
            partitioned_file_lists,
        )
        .projection_option(projection.cloned())
        .limit_option(limit)
        .build();

        let plan = self
            .options
            .create_physical_plan(state, file_scan_config)
            .await?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        datasources::{ExonFileType, ExonListingTableFactory},
        tests::test_listing_table_url,
    };

    use datafusion::{common::FileCompressionType, prelude::SessionContext};

    #[tokio::test]
    async fn test_bcf_read() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("bcf");

        let table = ExonListingTableFactory::default()
            .create_from_file_type(
                &session_state,
                ExonFileType::BCF,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
            )
            .await?;

        let df = ctx.read_table(table)?;

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();

            assert_eq!(batch.schema().fields().len(), 9);
            assert_eq!(batch.schema().field(0).name(), "chrom");
        }
        assert_eq!(row_cnt, 621);

        Ok(())
    }
}
