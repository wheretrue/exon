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
use exon_common::{TableSchemaBuilder, DEFAULT_BATCH_SIZE};
use object_store::ObjectStore;

pub fn new_gtf_schema_builder() -> TableSchemaBuilder {
    let file_fields = file_fields();
    TableSchemaBuilder::new_with_field_fields(file_fields)
}

/// The schema for a GTF file
fn file_fields() -> Vec<Field> {
    let attribute_key_field = Field::new("keys", DataType::Utf8, false);
    let attribute_value_field = Field::new("values", DataType::Utf8, true);

    vec![
        // https://useast.ensembl.org/info/website/upload/gff.html
        Field::new("seqname", DataType::Utf8, false),
        Field::new("source", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("score", DataType::Float32, true),
        Field::new("strand", DataType::Utf8, false),
        Field::new("frame", DataType::Utf8, true),
        Field::new_map(
            "attributes",
            "entries",
            attribute_key_field,
            attribute_value_field,
            false,
            true,
        ),
    ]
}

/// Configuration for a GTF data source.
pub struct GTFConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the GTF file. This is static.
    pub file_schema: SchemaRef,

    /// The object store to use for reading GTF files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl GTFConfig {
    /// Create a new GTF configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
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
