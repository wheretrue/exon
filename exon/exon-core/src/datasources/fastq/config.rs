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

/// Configuration for a FASTQ datasource.
pub struct FASTQConfig {
    /// The number of FASTQ records to read at a time.
    pub batch_size: usize,

    /// The schema of the FASTQ file.
    pub file_schema: SchemaRef,

    /// The object store to use for reading FASTQ files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the data.
    pub projection: Option<Vec<usize>>,
}

impl FASTQConfig {
    /// Create a new FASTQ configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            batch_size: crate::datasources::DEFAULT_BATCH_SIZE,
            object_store,
            file_schema: FASTQSchemaBuilder::default().build(),
            projection: None,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the projections.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        let file_projection = projection
            .iter()
            .filter(|f| **f < self.file_schema.fields().len())
            .cloned()
            .collect::<Vec<_>>();
        self.projection = Some(file_projection);
        self
    }
}

pub struct FASTQSchemaBuilder {
    fields: Vec<Field>,
}

impl FASTQSchemaBuilder {
    /// Extend the schema with the given fields.
    pub fn extend(mut self, fields: Vec<Field>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Build the schema.
    pub fn build(self) -> SchemaRef {
        Arc::new(Schema::new(self.fields))
    }
}

impl Default for FASTQSchemaBuilder {
    fn default() -> Self {
        let fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, true),
            Field::new("sequence", DataType::Utf8, false),
            Field::new("quality_scores", DataType::Utf8, false),
        ];

        Self { fields }
    }
}
