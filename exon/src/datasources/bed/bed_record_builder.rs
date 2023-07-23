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

use noodles::{
    bed::{
        record::{Color, Name, Score, Strand},
        Record,
    },
    core::Position,
};

pub struct BEDRecord {
    pub reference_sequence_name: String,
    pub start: u64,
    pub end: u64,
    pub name: Option<String>,
    pub score: Option<i64>,
    pub strand: Option<String>,
    pub thick_start: Option<u64>,
    pub thick_end: Option<u64>,
    pub color: Option<String>,
    pub block_count: Option<u64>,
    pub block_sizes: Option<String>,
    pub block_starts: Option<String>,
}

pub struct BEDRecordBuilder {
    reference_sequence_name: String,
    start: u64,
    end: u64,
    name: Option<String>,
    score: Option<i64>,
    strand: Option<String>,
    thick_start: Option<u64>,
    thick_end: Option<u64>,
    color: Option<String>,
    block_count: Option<u64>,
    block_sizes: Option<String>,
    block_starts: Option<String>,
}

impl BEDRecordBuilder {
    pub fn new() -> Self {
        BEDRecordBuilder {
            reference_sequence_name: String::new(),
            start: 0,
            end: 0,
            name: None,
            score: None,
            strand: None,
            thick_start: None,
            thick_end: None,
            color: None,
            block_count: None,
            block_sizes: None,
            block_starts: None,
        }
    }

    pub fn finish(self) -> BEDRecord {
        BEDRecord {
            reference_sequence_name: self.reference_sequence_name,
            start: self.start,
            end: self.end,
            name: self.name,
            score: self.score,
            strand: self.strand,
            thick_start: self.thick_start,
            thick_end: self.thick_end,
            color: self.color,
            block_count: self.block_count,
            block_sizes: self.block_sizes,
            block_starts: self.block_starts,
        }
    }

    pub fn reference_sequence_name(mut self, reference_sequence_name: String) -> Self {
        self.reference_sequence_name = reference_sequence_name;
        self
    }

    pub fn start(mut self, start: Position) -> Self {
        self.start = start.get() as u64;
        self
    }

    pub fn end(mut self, end: Position) -> Self {
        self.end = end.get() as u64;
        self
    }

    pub fn name(mut self, name: Option<&Name>) -> Self {
        self.name = name.map(|n| n.to_string());
        self
    }

    pub fn score(mut self, score: Option<Score>) -> Self {
        self.score = score.map(|i| u16::from(i) as i64);
        self
    }

    pub fn strand(mut self, strand: Option<Strand>) -> Self {
        self.strand = strand.map(|s| s.to_string());
        self
    }

    pub fn thick_start(mut self, thick_start: Position) -> Self {
        self.thick_start = Some(thick_start.get() as u64);
        self
    }

    pub fn thick_end(mut self, thick_end: Position) -> Self {
        self.thick_end = Some(thick_end.get() as u64);
        self
    }

    pub fn color(mut self, color: Option<Color>) -> Self {
        self.color = color.map(|c| c.to_string());
        self
    }

    pub fn block_count(mut self, block_count: Option<u64>) -> Self {
        self.block_count = block_count;
        self
    }

    pub fn block_sizes(mut self, block_sizes: Option<String>) -> Self {
        self.block_sizes = block_sizes;
        self
    }

    pub fn block_starts(mut self, block_starts: Option<String>) -> Self {
        self.block_starts = block_starts;
        self
    }
}

impl From<Record<12>> for BEDRecord {
    fn from(value: Record<12>) -> Self {
        let mut block_starts = Vec::new();
        let mut block_sizes = Vec::new();

        value.blocks().iter().for_each(|(start, size)| {
            block_starts.push(start.to_string());
            block_sizes.push(size.to_string());
        });

        let block_start_csv = block_starts.join(",");
        let block_size_csv = block_sizes.join(",");

        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.start_position())
            .end(value.end_position())
            .name(value.name())
            .score(value.score())
            .strand(value.strand())
            .thick_start(value.thick_start())
            .thick_end(value.thick_end())
            .color(value.color())
            .block_count(Some(block_starts.len() as u64))
            .block_sizes(Some(block_size_csv))
            .block_starts(Some(block_start_csv));

        builder.finish()
    }
}

impl From<Record<9>> for BEDRecord {
    fn from(value: Record<9>) -> Self {
        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.start_position())
            .end(value.end_position())
            .name(value.name())
            .score(value.score())
            .strand(value.strand())
            .thick_start(value.thick_start())
            .thick_end(value.thick_end())
            .color(value.color());

        builder.finish()
    }
}

impl From<Record<6>> for BEDRecord {
    fn from(value: Record<6>) -> Self {
        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.start_position())
            .end(value.end_position())
            .name(value.name())
            .score(value.score())
            .strand(value.strand());

        builder.finish()
    }
}
