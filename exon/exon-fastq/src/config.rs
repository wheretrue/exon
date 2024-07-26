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

use arrow::datatypes::{DataType, Field, SchemaRef};
use exon_common::TableSchemaBuilder;
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
            batch_size: exon_common::DEFAULT_BATCH_SIZE,
            object_store,
            file_schema: new_fastq_schema_builder().build().file_schema().unwrap(),
            projection: None,
        }
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Get the projection, returning the identity projection if none is set.
    pub fn projection(&self) -> Vec<usize> {
        self.projection
            .clone()
            .unwrap_or_else(|| (0..self.file_schema.fields().len()).collect())
    }

    /// Get the projected schema.
    pub fn projected_schema(&self) -> arrow::error::Result<SchemaRef> {
        let schema = self.file_schema.project(&self.projection())?;

        Ok(Arc::new(schema))
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

pub fn new_fastq_schema_builder() -> TableSchemaBuilder {
    let fields = vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_scores", DataType::Utf8, false),
    ];

    TableSchemaBuilder::new_with_field_fields(fields)
}
