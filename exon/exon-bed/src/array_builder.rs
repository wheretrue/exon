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

use arrow::{
    array::{ArrayRef, GenericStringBuilder, Int64Builder},
    datatypes::SchemaRef,
};
use exon_common::ExonArrayBuilder;

use super::bed_record_builder::BEDRecord;

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

    projection: Vec<usize>,

    rows: usize,
}

impl BEDArrayBuilder {
    pub fn create(schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

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
            projection,
            rows: 0,
        }
    }

    pub fn append(&mut self, record: BEDRecord) -> std::io::Result<()> {
        self.rows += 1;

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => self
                    .reference_sequence_names
                    .append_value(record.reference_sequence_name()),
                1 => self.starts.append_value(record.start() as i64),
                2 => self.ends.append_value(record.end() as i64),
                3 => self.names.append_option(record.name()),
                4 => self.scores.append_option(record.score()),
                5 => self.strands.append_option(record.strand()),
                6 => self
                    .thick_starts
                    .append_option(record.thick_start().map(|x| x as i64)),
                7 => self
                    .thick_ends
                    .append_option(record.thick_end().map(|x| x as i64)),
                8 => self.colors.append_option(record.color()),
                9 => self
                    .block_counts
                    .append_option(record.block_count().map(|x| x as i64)),
                10 => self.block_sizes.append_option(record.block_sizes()),
                11 => self.block_starts.append_option(record.block_starts()),
                _ => panic!("Invalid column index"),
            }
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = vec![];

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => arrays.push(Arc::new(self.reference_sequence_names.finish())),
                1 => arrays.push(Arc::new(self.starts.finish())),
                2 => arrays.push(Arc::new(self.ends.finish())),
                3 => arrays.push(Arc::new(self.names.finish())),
                4 => arrays.push(Arc::new(self.scores.finish())),
                5 => arrays.push(Arc::new(self.strands.finish())),
                6 => arrays.push(Arc::new(self.thick_starts.finish())),
                7 => arrays.push(Arc::new(self.thick_ends.finish())),
                8 => arrays.push(Arc::new(self.colors.finish())),
                9 => arrays.push(Arc::new(self.block_counts.finish())),
                10 => arrays.push(Arc::new(self.block_sizes.finish())),
                11 => arrays.push(Arc::new(self.block_starts.finish())),
                _ => panic!("Invalid column index"),
            }
        }

        arrays
    }
}

impl ExonArrayBuilder for BEDArrayBuilder {
    fn finish(&mut self) -> Vec<ArrayRef> {
        self.finish()
    }

    fn len(&self) -> usize {
        self.rows
    }
}
