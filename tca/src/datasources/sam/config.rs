use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

/// Configuration for a SAM datasource.
pub struct SAMConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the SAM file.
    pub file_schema: SchemaRef,

    /// The object store to use for reading SAM files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl SAMConfig {
    /// Create a new SAM configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
            file_schema,
            object_store,
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
