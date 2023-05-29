use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

/// Configuration for a VCF datasource.
pub struct VCFConfig {
    /// The number of records to read at a time.
    pub batch_size: usize,
    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,
    /// The file schema to use.
    pub file_schema: Arc<arrow::datatypes::Schema>,
    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl VCFConfig {
    /// Create a new VCF configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
            object_store,
            file_schema,
            projection: None,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the projection.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }
}
