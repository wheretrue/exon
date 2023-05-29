use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::file_format::{file_type::FileCompressionType, FileFormat},
    execution::context::SessionState,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan, PhysicalExpr, Statistics},
};
use object_store::{ObjectMeta, ObjectStore};

use super::scanner::GenbankScan;

#[derive(Debug)]
/// Implements a datafusion `FileFormat` for Genbank files.
pub struct GenbankFormat {
    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl Default for GenbankFormat {
    fn default() -> Self {
        Self {
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

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
        Field::new("division", DataType::Utf8, true),
        Field::new("keywords", DataType::Utf8, true),
        Field::new("molecule_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("topology", DataType::Utf8, true),
        Field::new("features", DataType::List(Arc::new(feature_field)), true),
    ]);

    Arc::new(schema)
}

impl GenbankFormat {
    /// Create a new Genbank format.
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        Self {
            file_compression_type,
        }
    }

    /// Return the schema for the Genbank format.
    pub fn schema(&self) -> datafusion::error::Result<SchemaRef> {
        Ok(schema())
    }
}

#[async_trait]
impl FileFormat for GenbankFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        self.schema()
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let scan = GenbankScan::new(conf, self.file_compression_type.clone());

        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::tests::test_listing_table_url;

    use super::GenbankFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_listing() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("genbank/test.gb");

        let genbank_format = Arc::new(GenbankFormat::default());
        let lo = ListingOptions::new(genbank_format.clone());

        let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

        let config = ListingTableConfig::new(table_path)
            .with_listing_options(lo)
            .with_schema(resolved_schema);

        let provider = Arc::new(ListingTable::try_new(config).unwrap());
        let df = ctx.read_table(provider.clone()).unwrap();

        let mut row_cnt = 0;
        let bs = df.collect().await.unwrap();
        for batch in bs {
            row_cnt += batch.num_rows();
        }
        assert_eq!(row_cnt, 1)
    }
}
