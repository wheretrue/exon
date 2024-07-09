// Copyright 2024 WHERE TRUE Technologies.
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

use arrow::datatypes::{Field, Schema};
use exon_common::TableSchema;

use crate::record::Data;

/// Builds a schema for an SDF file.
pub struct SDFSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

impl Default for SDFSchemaBuilder {
    fn default() -> Self {
        // by default, data is a struct with a single field, which is a string called "canonical_smiles"
        let data_fields = vec![Field::new(
            "canonical_smiles",
            arrow::datatypes::DataType::Utf8,
            false,
        )];
        let struct_type = arrow::datatypes::DataType::Struct(data_fields.into());

        let file_fields = vec![
            // header which is a string
            Field::new("header", arrow::datatypes::DataType::Utf8, false),
            // atom count which is a 32-bit unsigned integer
            Field::new("atom_count", arrow::datatypes::DataType::UInt32, false),
            // bond count which is a 32-bit unsigned integer
            Field::new("bond_count", arrow::datatypes::DataType::UInt32, false),
            // data which is a struct with a single field, which is a string called "canonical_smiles"
            Field::new("data", struct_type, false),
        ];

        Self {
            file_fields,
            partition_fields: Vec::new(),
        }
    }
}

impl SDFSchemaBuilder {
    /// Creates a new schema builder.
    pub fn new() -> Self {
        SDFSchemaBuilder {
            file_fields: Vec::new(),
            partition_fields: Vec::new(),
        }
    }

    /// Adds a field to the schema.
    pub fn add_field(&mut self, field: Field) {
        self.file_fields.push(field);
    }

    /// Adds a partition field to the schema.
    pub fn add_partition_field(&mut self, field: Field) {
        self.partition_fields.push(field);
    }

    /// Update the data field based on the input data.
    pub fn update_data_field(&mut self, data: &Data) {
        let new_fields = data
            .into_iter()
            .map(|d| Field::new(d.header(), arrow::datatypes::DataType::Utf8, true))
            .collect::<Vec<_>>();

        let struct_type = arrow::datatypes::DataType::Struct(new_fields.into());
        self.file_fields[3] = Field::new("data", struct_type, false);
    }

    /// Builds the schema.
    pub fn build(self) -> TableSchema {
        let mut fields = self.file_fields.clone();
        fields.extend_from_slice(&self.partition_fields);

        let schema = Schema::new(fields);

        let projection = (0..self.file_fields.len()).collect::<Vec<_>>();

        TableSchema::new(Arc::new(schema), projection)
    }
}
