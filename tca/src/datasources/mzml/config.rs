use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use object_store::ObjectStore;

/// Configuration for a MzML data source.
pub struct MzMLConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema to use for MzML files.
    pub file_schema: Arc<Schema>,

    /// The object store to use for reading MzML files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl MzMLConfig {
    /// Create a new MzML configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
            file_schema: Arc::new(schema()),
            projection: None,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the projection.
    pub fn with_some_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }
}

impl Default for MzMLConfig {
    fn default() -> Self {
        Self {
            object_store: Arc::new(object_store::local::LocalFileSystem::new()),
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
            file_schema: Arc::new(schema()),
            projection: None,
        }
    }
}

pub fn schema() -> Schema {
    Schema::new(vec![Field::new("id", DataType::Utf8, false)])
}
