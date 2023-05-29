use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

use crate::datasources::DEFAULT_BATCH_SIZE;

use super::file_format::schema;

/// Configuration for a GFF data source.
pub struct GFFConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the GFF file. This is static.
    pub file_schema: SchemaRef,

    /// The object store to use for reading GFF files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl GFFConfig {
    /// Create a new GFF configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        let file_schema = schema();

        Self {
            file_schema,
            object_store,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: None,
        }
    }

    /// Set the file schema.
    pub fn with_schema(mut self, file_schema: SchemaRef) -> Self {
        self.file_schema = file_schema;
        self
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
