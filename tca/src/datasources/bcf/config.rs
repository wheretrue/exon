use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

/// Configuration for a BCF datasource.
pub struct BCFConfig {
    /// The object store to use for reading BCF files.
    pub object_store: Arc<dyn ObjectStore>,

    /// The number of records to read at a time.
    pub batch_size: usize,

    /// The file schema to use.
    pub file_schema: Arc<arrow::datatypes::Schema>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl BCFConfig {
    /// Create a new BCF configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            object_store,
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
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

    /// Set the projection from an optional vector.
    pub fn with_some_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }
}
