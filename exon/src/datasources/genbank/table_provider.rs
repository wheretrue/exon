use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
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

use crate::{
    datasources::ExonFileType, physical_plan::file_scan_config_builder::FileScanConfigBuilder,
};

use super::GenbankScan;

/// The schema for a Genbank file
pub fn schema() -> SchemaRef {
    let kind_field = Field::new("kind", DataType::Utf8, false);
    let location_field = Field::new("location", DataType::Utf8, false);

    let qualifier_key_field = Field::new("keys", DataType::Utf8, false);
    let qualifier_value_field = Field::new("values", DataType::Utf8, true);
    let qualifiers_field = Field::new_map(
        "qualifiers",
        "entries",
        qualifier_key_field,
        qualifier_value_field,
        false,
        true,
    );

    let fields = Fields::from(vec![kind_field, location_field, qualifiers_field]);
    let feature_field = Field::new("item", DataType::Struct(fields), true);

    let comment_field = Field::new("item", DataType::Utf8, true);

    let schema = Schema::new(vec![
        Field::new("sequence", DataType::Utf8, false),
        Field::new("accession", DataType::Utf8, true),
        Field::new("comments", DataType::List(Arc::new(comment_field)), true),
        Field::new("contig", DataType::Utf8, true),
        Field::new("date", DataType::Utf8, true),
        Field::new("dblink", DataType::Utf8, true),
        Field::new("definition", DataType::Utf8, true),
        Field::new("division", DataType::Utf8, false),
        Field::new("keywords", DataType::Utf8, true),
        Field::new("molecule_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("topology", DataType::Utf8, false),
        Field::new("features", DataType::List(Arc::new(feature_field)), true),
    ]);

    Arc::new(schema)
}

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingGenbankTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingGenbankTableOptions>,
}

impl ListingGenbankTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingGenbankTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a Genbank table
pub struct ListingGenbankTableOptions {
    file_extension: String,

    file_compression_type: FileCompressionType,
}

impl ListingGenbankTableOptions {
    /// Create a new set of options
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        let file_extension = ExonFileType::FASTA.get_file_extension(file_compression_type);

        Self {
            file_extension,
            file_compression_type,
        }
    }

    /// Infer the schema for the table
    pub async fn infer_schema(&self) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();

        Ok(schema)
    }

    async fn create_physical_plan(
        &self,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = GenbankScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A table provider for a Genbank listing table
pub struct ListingGenbankTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingGenbankTableOptions,
}

impl ListingGenbankTable {
    /// Create a new Genbank listing table
    pub fn try_new(config: ListingGenbankTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
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
impl TableProvider for ListingGenbankTable {
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

        let file_scan_config = FileScanConfigBuilder::new(
            object_store_url.clone(),
            Arc::clone(&self.table_schema),
            partitioned_file_lists,
        )
        .projection_option(projection.cloned())
        .limit_option(limit)
        .build();

        let plan = self.options.create_physical_plan(file_scan_config).await?;

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
    async fn test_listing() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("genbank/test.gb");
        let table = ExonListingTableFactory::new()
            .create_from_file_type(
                &session_state,
                ExonFileType::GENBANK,
                FileCompressionType::UNCOMPRESSED,
                table_path.to_string(),
            )
            .await?;

        let df = ctx.read_table(table)?;

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 1);

        Ok(())
    }
}