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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use object_store::ObjectStore;

use crate::datasources::DEFAULT_BATCH_SIZE;

/// Configuration for a FASTA data source.
pub struct FASTAConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the FASTA file.
    pub file_schema: SchemaRef,

    /// The object store to use for reading FASTA files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl FASTAConfig {
    /// Create a new FASTA configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            object_store,
            file_schema,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: None,
        }
    }

    /// Create a new FASTA configuration with a given batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Create a new FASTA configuration with a given projection.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }
}

impl Default for FASTAConfig {
    fn default() -> Self {
        Self {
            object_store: Arc::new(object_store::local::LocalFileSystem::new()),
            file_schema: Arc::new(schema()),
            batch_size: DEFAULT_BATCH_SIZE,
            projection: None,
        }
    }
}

/// FASTA Schema
pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("sequence", DataType::Utf8, false),
    ])
}
