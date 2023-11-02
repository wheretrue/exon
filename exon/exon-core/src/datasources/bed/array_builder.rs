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

use arrow::{
    array::{ArrayBuilder, ArrayRef, GenericStringBuilder, Int64Builder},
    datatypes::{DataType, Field, Schema},
};

use super::bed_record_builder::BEDRecord;

pub(crate) struct BEDSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
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
    pub fn build(self) -> (Schema, Vec<usize>) {
        let mut fields = self.file_fields.clone();
        fields.extend_from_slice(&self.partition_fields);

        let schema = Schema::new(fields);

        let projection = (0..self.file_fields.len()).collect::<Vec<_>>();

        (schema, projection)
    }
}

impl Default for BEDSchemaBuilder {
    fn default() -> Self {
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

        Self::new(field_fields, vec![])
    }
}

pub struct BEDArrayBuilder {
    reference_sequence_names: GenericStringBuilder<i32>,
    starts: Int64Builder,
    ends: Int64Builder,
    names: GenericStringBuilder<i32>,
    scores: Int64Builder,
    strands: GenericStringBuilder<i32>,
    thick_starts: Int64Builder,
    thick_ends: Int64Builder,
    colors: GenericStringBuilder<i32>,
    block_counts: Int64Builder,
    block_sizes: GenericStringBuilder<i32>,
    block_starts: GenericStringBuilder<i32>,
}

impl BEDArrayBuilder {
    pub fn create() -> Self {
        Self {
            reference_sequence_names: GenericStringBuilder::<i32>::new(),
            starts: Int64Builder::new(),
            ends: Int64Builder::new(),
            names: GenericStringBuilder::<i32>::new(),
            scores: Int64Builder::new(),
            strands: GenericStringBuilder::<i32>::new(),
            thick_starts: Int64Builder::new(),
            thick_ends: Int64Builder::new(),
            colors: GenericStringBuilder::<i32>::new(),
            block_counts: Int64Builder::new(),
            block_sizes: GenericStringBuilder::<i32>::new(),
            block_starts: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.reference_sequence_names.len()
    }

    pub fn append(&mut self, record: BEDRecord) -> std::io::Result<()> {
        self.reference_sequence_names
            .append_value(record.reference_sequence_name.as_str());

        self.starts.append_value(record.start as i64);
        self.ends.append_value(record.end as i64);

        self.names.append_option(record.name);
        self.scores.append_option(record.score);

        self.strands.append_option(record.strand);
        self.thick_starts
            .append_option(record.thick_start.map(|x| x as i64));
        self.thick_ends
            .append_option(record.thick_end.map(|x| x as i64));

        self.colors.append_option(record.color);
        self.block_counts
            .append_option(record.block_count.map(|x| x as i64));

        self.block_sizes.append_option(record.block_sizes);
        self.block_starts.append_option(record.block_starts);

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let reference_sequence_names = self.reference_sequence_names.finish();
        let starts = self.starts.finish();
        let ends = self.ends.finish();
        let names = self.names.finish();
        let scores = self.scores.finish();
        let strands = self.strands.finish();
        let thick_starts = self.thick_starts.finish();
        let thick_ends = self.thick_ends.finish();
        let colors = self.colors.finish();
        let block_counts = self.block_counts.finish();
        let block_sizes = self.block_sizes.finish();
        let block_starts = self.block_starts.finish();

        vec![
            Arc::new(reference_sequence_names),
            Arc::new(starts),
            Arc::new(ends),
            Arc::new(names),
            Arc::new(scores),
            Arc::new(strands),
            Arc::new(thick_starts),
            Arc::new(thick_ends),
            Arc::new(colors),
            Arc::new(block_counts),
            Arc::new(block_sizes),
            Arc::new(block_starts),
        ]
    }
}
