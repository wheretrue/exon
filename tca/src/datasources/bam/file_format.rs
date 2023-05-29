use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::file_format::FileFormat,
    execution::context::SessionState,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan, PhysicalExpr, Statistics},
};
use object_store::{ObjectMeta, ObjectStore};

use super::{array_builder::schema, scanner::BAMScan};

#[derive(Debug, Default)]
/// Implements a datafusion `FileFormat` for BAM files.
pub struct BAMFormat {}

#[async_trait]
impl FileFormat for BAMFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schema = schema();

        Ok(Arc::new(schema))
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
        let scan = BAMScan::new(conf);
        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::BAMFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_schema_inference() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = ListingTableUrl::parse("test-data").unwrap();

        let fasta_format = Arc::new(BAMFormat::default());
        let lo = ListingOptions::new(fasta_format.clone()).with_file_extension("bam");

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
