use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

use crate::datasources::DEFAULT_BATCH_SIZE;

/// Configuration for a BED datasource.
#[derive(Debug)]
pub struct BEDConfig {
    /// The number of records to read at a time.
    pub batch_size: usize,

    /// The schema of the BED file.
    pub file_schema: SchemaRef,

    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl BEDConfig {
    /// Create a new BED configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
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

    /// Set the projection from an optional vector.
    pub fn with_some_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }
}
