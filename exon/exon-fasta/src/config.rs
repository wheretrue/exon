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
use exon_common::TableSchema;
use object_store::ObjectStore;

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

    /// How many bytes to pre-allocate for the sequence.
    pub fasta_sequence_buffer_capacity: usize,

    /// Whether or not to use a LargeUtf8 array for the sequence.
    pub use_large_utf8: bool,
}

impl FASTAConfig {
    /// Create a new FASTA configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            object_store,
            file_schema,
            batch_size: exon_common::DEFAULT_BATCH_SIZE,
            projection: None,
            fasta_sequence_buffer_capacity: 384,
            use_large_utf8: false,
        }
    }

    /// Create a new FASTA configuration with a given batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Create a new FASTA configuration with a given projection.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        // Only include fields that are in the file schema.
        // TODO: make this cleaner, i.e. projection should probably come
        // pre-filtered.
        let file_projection = projection
            .iter()
            .filter(|f| **f < self.file_schema.fields().len())
            .cloned()
            .collect::<Vec<_>>();

        self.projection = Some(file_projection);
        self
    }

    /// Create a new FASTA configuration with a given sequence capacity.
    pub fn with_fasta_sequence_buffer_capacity(
        mut self,
        fasta_sequence_buffer_capacity: usize,
    ) -> Self {
        self.fasta_sequence_buffer_capacity = fasta_sequence_buffer_capacity;
        self
    }

    /// Create a new FASTA configuration with the use_large_utf8 flag set.
    pub fn with_use_large_utf8(mut self, use_large_utf8: bool) -> Self {
        self.use_large_utf8 = use_large_utf8;
        self
    }
}

pub struct FASTASchemaBuilder {
    /// The fields of the schema.
    fields: Vec<Field>,

    /// The partition fields to potentially add to the schema.
    partition_fields: Vec<Field>,

    /// Whether or not to use a LargeUtf8 array for the sequence.
    large_utf8: bool,
}

impl Default for FASTASchemaBuilder {
    fn default() -> Self {
        Self {
            fields: vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, true),
                Field::new("sequence", DataType::Utf8, false),
            ],
            partition_fields: vec![],
            large_utf8: false,
        }
    }
}

impl FASTASchemaBuilder {
    /// Set the large_utf8 flag.
    pub fn with_large_utf8(mut self, large_utf8: bool) -> Self {
        self.large_utf8 = large_utf8;
        self
    }

    /// Extend the partition fields with the given fields.
    pub fn with_partition_fields(mut self, partition_fields: Vec<Field>) -> Self {
        self.partition_fields.extend(partition_fields);
        self
    }

    pub fn build(&mut self) -> TableSchema {
        if self.large_utf8 {
            let field = Field::new("sequence", DataType::LargeUtf8, false);
            self.fields[2] = field;
        }

        let file_field_projection = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect::<Vec<_>>();

        self.fields.extend(self.partition_fields.clone());

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(self.fields.clone()));
        TableSchema::new(arrow_schema, file_field_projection)
    }
}
