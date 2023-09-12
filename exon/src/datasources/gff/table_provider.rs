use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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

use super::GFFScan;

#[derive(Debug, Clone)]
/// Configuration for a VCF listing table
pub struct ListingGFFTableConfig {
    inner: ListingTableConfig,

    options: Option<ListingGFFTableOptions>,
}

impl ListingGFFTableConfig {
    /// Create a new VCF listing table configuration
    pub fn new(table_path: ListingTableUrl) -> Self {
        Self {
            inner: ListingTableConfig::new(table_path),
            options: None,
        }
    }

    /// Set the options for the VCF listing table
    pub fn with_options(self, options: ListingGFFTableOptions) -> Self {
        Self {
            options: Some(options),
            ..self
        }
    }
}

#[derive(Debug, Clone)]
/// Listing options for a GFF table
pub struct ListingGFFTableOptions {
    file_extension: String,

    file_compression_type: FileCompressionType,
}

impl ListingGFFTableOptions {
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
        let scan = GFFScan::new(conf.clone(), self.file_compression_type);

        Ok(Arc::new(scan))
    }
}

#[derive(Debug, Clone)]
/// A GFF listing table
pub struct ListingGFFTable {
    table_paths: Vec<ListingTableUrl>,

    table_schema: SchemaRef,

    options: ListingGFFTableOptions,
}

impl ListingGFFTable {
    /// Create a new VCF listing table
    pub fn try_new(config: ListingGFFTableConfig, table_schema: Arc<Schema>) -> Result<Self> {
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
impl TableProvider for ListingGFFTable {
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

/// Schema for a GFF file
pub fn schema() -> SchemaRef {
    let attribute_key_field = Field::new("keys", DataType::Utf8, false);

    // attribute_value_field is a list of strings
    let value_field = Field::new("item", DataType::Utf8, true);
    let attribute_value_field = Field::new("values", DataType::List(Arc::new(value_field)), true);

    let inner = Schema::new(vec![
        Field::new("seqname", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, true),
        Field::new_map(
            "attributes",
            "entries",
            attribute_key_field,
            attribute_value_field,
            false,
            true,
        ),
    ]);

    inner.into()
}

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;

//     use datafusion::{
//         datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
//         prelude::SessionContext,
//     };

//     #[tokio::test]
//     async fn test_listing() {
//         let ctx = SessionContext::new();
//         let session_state = ctx.state();

//         let table_path = ListingTableUrl::parse("test-data/datasources/gff").unwrap();

//         let gff_format = Arc::new(GFFFormat::default());
//         let lo = ListingOptions::new(gff_format.clone()).with_file_extension("gff");

//         let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

//         let config = ListingTableConfig::new(table_path)
//             .with_listing_options(lo)
//             .with_schema(resolved_schema);

//         let provider = Arc::new(ListingTable::try_new(config).unwrap());
//         let df = ctx.read_table(provider.clone()).unwrap();

//         let mut row_cnt = 0;
//         let bs = df.collect().await.unwrap();
//         for batch in bs {
//             row_cnt += batch.num_rows();
//         }
//         assert_eq!(row_cnt, 5000)
//     }
// }
