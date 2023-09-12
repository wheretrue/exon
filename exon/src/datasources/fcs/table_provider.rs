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
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

use crate::{datasources::ExonFileType, io::exon_object_store};

use super::{reader::FcsReader, scanner::FCSScan};

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingFCSTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingFCSTableOptions>,
}

impl ListingFCSTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingFCSTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for an FCS table
pub struct ListingFCSTableOptions {
    file_extension: String,

    file_compression_type: FileCompressionType,
}

impl ListingFCSTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::FASTA.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(
        &self,
        state: &SessionState,
        table_path: &ListingTableUrl,
    ) -> datafusion::error::Result<SchemaRef> {
        let store = state.runtime_env().object_store(table_path)?;

        let get_result = store.get(table_path.prefix()).await?;

        let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
        let stream_reader = StreamReader::new(stream_reader);

        let fcs_reader = FcsReader::new(stream_reader).await?;
        let parameter_names = fcs_reader.text_data.parameter_names();

        // Create the schema assuming all columns are of type `Float32`
        let mut fields = Vec::with_capacity(parameter_names.len());
        for parameter_name in parameter_names {
            fields.push(arrow::datatypes::Field::new(
                parameter_name,
                arrow::datatypes::DataType::Float32,
                false,
            ));
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));

        Ok(schema)
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = FCSScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A FCS listing table
pub struct ListingFCSTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingFCSTableOptions,
}

impl ListingFCSTable {
    /// Create a new FCS listing table
    pub fn try_new(config: ListingFCSTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
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
impl TableProvider for ListingFCSTable {
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

        let plan = self.options.create_physical_plan(file_scan_config).await?;

        Ok(plan)
    }
}
