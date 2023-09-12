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
use futures::TryStreamExt;
use noodles::bcf;
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

use crate::{
    datasources::{vcf::VCFSchemaBuilder, ExonFileType},
    io::exon_object_store,
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
}

impl ListingBCFTableOptions {
    /// Create a new set of options
    pub fn new() -> Self {
        let file_extension =
            ExonFileType::BCF.get_file_extension(FileCompressionType::UNCOMPRESSED);

        Self { file_extension }
    }

    /// Infer the schema for the table
    pub async fn infer_schema<'a>(
        &self,
        state: &SessionState,
        table_path: &'a ListingTableUrl,
    ) -> datafusion::error::Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let get_result = if table_path.to_string().ends_with("/") {
            let list = store.list(Some(table_path.prefix())).await?;
            let collected_list = list.try_collect::<Vec<_>>().await?;
            let first = collected_list
                .first()
                .ok_or_else(|| DataFusionError::Execution("No files found".to_string()))?;

            let get_result = store.get(&first.location).await?;

            get_result
        } else {
            let head = store.head(table_path.prefix()).await?;

            let get_result = store.get(&head.location).await?;

            get_result
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
        let scan = BCFScan::new(conf.clone());

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
            exon_object_store::list_files_for_scan(
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

        let plan = self
            .options
            .create_physical_plan(state, file_scan_config)
            .await?;

        Ok(plan)
    }
}
