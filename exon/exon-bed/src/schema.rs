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

use arrow::datatypes::{DataType, Field, Schema};
use exon_common::TableSchema;

use crate::{ExonBEDError, ExonBEDResult};

pub struct BEDSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

fn file_fields(n_fields: usize) -> ExonBEDResult<Vec<Field>> {
    if n_fields < 3 || n_fields > 12 {
        return Err(ExonBEDError::InvalidNumberOfFields(n_fields));
    }

    let field_fields = vec![
        Field::new("reference_sequence_name", DataType::Utf8, false),
        Field::new("start", DataType::Int64, false),
        Field::new("end", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Int64, true),
        Field::new("strand", DataType::Utf8, true),
        Field::new("thick_start", DataType::Int64, true),
        Field::new("thick_end", DataType::Int64, true),
        Field::new("color", DataType::Utf8, true),
        Field::new("block_count", DataType::Int64, true),
        Field::new("block_sizes", DataType::Utf8, true),
        Field::new("block_starts", DataType::Utf8, true),
    ];

    Ok(field_fields[0..n_fields].to_vec())
}

impl BEDSchemaBuilder {
    pub fn new(file_fields: Vec<Field>, partition_fields: Vec<Field>) -> Self {
        Self {
            file_fields,
            partition_fields,
        }
    }

    pub fn add_partition_fields(&mut self, fields: Vec<Field>) {
        self.partition_fields.extend(fields);
    }

    /// Returns the schema and the projection indexes for the file's schema
    pub fn build(self) -> TableSchema {
        let mut fields = self.file_fields.clone();
        fields.extend_from_slice(&self.partition_fields);

        let schema = Schema::new(fields);

        let projection = (0..self.file_fields.len()).collect::<Vec<_>>();

        TableSchema::new(Arc::new(schema), projection)
    }

    /// From number of fields, create a schema with default fields
    pub fn with_n_fields(n_fields: usize) -> ExonBEDResult<Self> {
        let field_fields = file_fields(n_fields)?;

        Ok(Self::new(field_fields, vec![]))
    }
}

impl Default for BEDSchemaBuilder {
    fn default() -> Self {
        let field_fields = file_fields(12).unwrap();
        Self::new(field_fields, vec![])
    }
}
