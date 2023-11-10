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
use exon_common::TableSchemaBuilder;
use object_store::ObjectStore;

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
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: Arc<Schema>) -> Self {
        Self {
            file_schema,
            object_store,
            batch_size: 8096,
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
        let file_projection = projection
            .iter()
            .filter(|f| **f < self.file_schema.fields().len())
            .cloned()
            .collect::<Vec<_>>();

        self.projection = Some(file_projection);
        self
    }
}

pub fn new_gff_schema_builder() -> TableSchemaBuilder {
    let attribute_key_field = Field::new("keys", DataType::Utf8, false);

    // attribute_value_field is a list of strings
    let value_field = Field::new("item", DataType::Utf8, true);
    let attribute_value_field = Field::new("values", DataType::List(Arc::new(value_field)), true);

    let fields = vec![
        Field::new("seqname", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("phase", DataType::Utf8, true),
        Field::new_map(
            "attributes",
            "entries",
            attribute_key_field,
            attribute_value_field,
            false,
            true,
        ),
    ];

    TableSchemaBuilder::new_with_field_fields(fields)
}
