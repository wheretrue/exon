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

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use exon_common::DEFAULT_BATCH_SIZE;
use object_store::ObjectStore;

/// The schema for a Genbank file
pub fn schema() -> SchemaRef {
    let kind_field = Field::new("kind", DataType::Utf8, false);
    let location_field = Field::new("location", DataType::Utf8, false);

    let qualifier_key_field = Field::new("keys", DataType::Utf8, false);
    let qualifier_value_field = Field::new("values", DataType::Utf8, true);
    let qualifiers_field = Field::new_map(
        "qualifiers",
        "entries",
        qualifier_key_field,
        qualifier_value_field,
        false,
        true,
    );

    let fields = Fields::from(vec![kind_field, location_field, qualifiers_field]);
    let feature_field = Field::new("item", DataType::Struct(fields), true);

    let comment_field = Field::new("item", DataType::Utf8, true);

    let schema = Schema::new(vec![
        Field::new("sequence", DataType::Utf8, false),
        Field::new("accession", DataType::Utf8, true),
        Field::new("comments", DataType::List(Arc::new(comment_field)), true),
        Field::new("contig", DataType::Utf8, true),
        Field::new("date", DataType::Utf8, true),
        Field::new("dblink", DataType::Utf8, true),
        Field::new("definition", DataType::Utf8, true),
        Field::new("division", DataType::Utf8, false),
        Field::new("keywords", DataType::Utf8, true),
        Field::new("molecule_type", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("source", DataType::Utf8, true),
        Field::new("version", DataType::Utf8, true),
        Field::new("topology", DataType::Utf8, false),
        Field::new("features", DataType::List(Arc::new(feature_field)), true),
    ]);

    Arc::new(schema)
}

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
            batch_size: DEFAULT_BATCH_SIZE,
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
