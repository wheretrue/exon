use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    datasource::file_format::{file_type::FileCompressionType, FileFormat},
    execution::context::SessionState,
    physical_plan::{file_format::FileScanConfig, ExecutionPlan, PhysicalExpr, Statistics},
};
use object_store::{ObjectMeta, ObjectStore};

use super::{config::schema, scanner::FASTAScan};

#[derive(Debug)]
/// Implements a datafusion `FileFormat` for FASTA files.
pub struct FASTAFormat {
    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl Default for FASTAFormat {
    fn default() -> Self {
        Self {
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

impl FASTAFormat {
    /// Create a new FASTA format.
    pub fn new(file_compression_type: FileCompressionType) -> Self {
        Self {
            file_compression_type,
        }
    }
}

#[async_trait]
impl FileFormat for FASTAFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let s = schema();

        Ok(Arc::new(s))
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
        let scan = FASTAScan::new(conf, self.file_compression_type.clone());
        Ok(Arc::new(scan))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::tests::test_listing_table_url;

    use super::FASTAFormat;
    use datafusion::{
        datasource::listing::{ListingOptions, ListingTable, ListingTableConfig},
        prelude::SessionContext,
    };

    #[tokio::test]
    async fn test_listing() {
        let ctx = SessionContext::new();
        let session_state = ctx.state();

        let table_path = test_listing_table_url("fasta/test.fasta");

        let fasta_format = Arc::new(FASTAFormat::default());
        let lo = ListingOptions::new(fasta_format.clone()).with_file_extension("fasta");

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
        assert_eq!(row_cnt, 2)
    }
}
