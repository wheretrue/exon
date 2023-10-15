// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

use super::table_provider::schema;

/// Configuration for a Genbank data source.
pub struct GenbankConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the Genbank file. This is static.
    pub file_schema: SchemaRef,

    /// The object store to use for reading Genbank files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl GenbankConfig {
    /// Create a new Genbank configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            file_schema: schema(),
            object_store,
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
            projection: None,
        }
    }

    /// Set the file schema.
    pub fn with_file_schema(mut self, file_schema: SchemaRef) -> Self {
        self.file_schema = file_schema;
        self
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
