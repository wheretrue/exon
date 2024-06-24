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

pub struct HMMDomTabSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

impl HMMDomTabSchemaBuilder {
    pub fn add_partition_fields(&mut self, partition_fields: Vec<Field>) {
        self.partition_fields.extend(partition_fields);
    }

    pub fn build(self) -> TableSchema {
        let mut fields = self.file_fields.clone();
        fields.extend(self.partition_fields);

        let schema = Schema::new(fields);

        let projection: Vec<usize> = (0..self.file_fields.len()).collect();

        TableSchema::new(Arc::new(schema.clone()), projection.clone())
    }
}

fn file_fields() -> Vec<Field> {
    vec![
        Field::new("target_name", DataType::Utf8, false),
        Field::new("target_accession", DataType::Utf8, false),
        Field::new("tlen", DataType::Int64, false),
        Field::new("query_name", DataType::Utf8, false),
        Field::new("accession", DataType::Utf8, false),
        Field::new("qlen", DataType::Int64, false),
        Field::new("evalue", DataType::Float64, false),
        Field::new("sequence_score", DataType::Float64, false),
        Field::new("bias", DataType::Float64, false),
        Field::new("domain_number", DataType::Int64, false),
        Field::new("ndom", DataType::Int64, false),
        Field::new("conditional_evalue", DataType::Float64, false),
        Field::new("independent_evalue", DataType::Float64, false),
        Field::new("domain_score", DataType::Float64, false),
        Field::new("domain_bias", DataType::Float64, false),
        Field::new("hmm_from", DataType::Int64, false),
        Field::new("hmm_to", DataType::Int64, false),
        Field::new("ali_from", DataType::Int64, false),
        Field::new("ali_to", DataType::Int64, false),
        Field::new("env_from", DataType::Int64, false),
        Field::new("env_to", DataType::Int64, false),
        Field::new("accuracy", DataType::Float64, false),
        Field::new("description", DataType::Utf8, false),
    ]
}

impl Default for HMMDomTabSchemaBuilder {
    fn default() -> Self {
        Self {
            file_fields: file_fields(),
            partition_fields: vec![],
        }
    }
}
