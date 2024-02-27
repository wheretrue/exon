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

use crate::error::Result;
use arrow::datatypes::Field;
use exon_common::TableSchema;

pub(crate) struct CRAMSchemaBuilder {
    /// The fields of the schema.
    fields: Vec<arrow::datatypes::Field>,

    /// The partition fields to potentially include.
    partition_fields: Vec<arrow::datatypes::Field>,

    /// The header to use for schema inference.
    header: Option<String>,
}

impl Default for CRAMSchemaBuilder {
    fn default() -> Self {
        Self {
            fields: vec![
                Field::new("chrom", arrow::datatypes::DataType::Utf8, false),
                Field::new("pos", arrow::datatypes::DataType::Int64, false),
            ],
            partition_fields: vec![],
            header: None,
        }
    }
}

impl CRAMSchemaBuilder {
    pub fn build(&mut self) -> Result<TableSchema> {
        let mut fields = self.fields.clone();
        let file_projection = self.fields.iter().enumerate().map(|(i, _)| i).collect();

        fields.extend(self.partition_fields.clone());

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Ok(TableSchema::new(schema, file_projection))
    }
}
