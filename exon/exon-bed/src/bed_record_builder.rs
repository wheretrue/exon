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

use noodles::{
    bed::feature::{record::Strand, RecordBuf},
    core::Position,
};

use bstr::BStr;

pub struct BEDRecord {
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

impl BEDRecord {
    pub fn reference_sequence_name(&self) -> &str {
        &self.reference_sequence_name
    }

    pub fn start(&self) -> u64 {
        self.start
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub fn score(&self) -> Option<i64> {
        self.score
    }

    pub fn strand(&self) -> Option<&str> {
        self.strand.as_deref()
    }

    pub fn thick_start(&self) -> Option<u64> {
        self.thick_start
    }

    pub fn thick_end(&self) -> Option<u64> {
        self.thick_end
    }

    pub fn color(&self) -> Option<&str> {
        self.color.as_deref()
    }

    pub fn block_count(&self) -> Option<u64> {
        self.block_count
    }

    pub fn block_sizes(&self) -> Option<&str> {
        self.block_sizes.as_deref()
    }

    pub fn block_starts(&self) -> Option<&str> {
        self.block_starts.as_deref()
    }
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

    pub fn name(mut self, name: Option<&BStr>) -> Self {
        self.name = name.map(|n| n.to_string());
        self
    }

    pub fn score(mut self, score: Option<u16>) -> Self {
        self.score = score.map(|i| u16::from(i) as i64);
        self
    }

    pub fn strand(mut self, strand: Option<Strand>) -> Self {
        self.strand = match strand {
            Some(Strand::Forward) => Some("+".to_string()),
            Some(Strand::Reverse) => Some("-".to_string()),
            None => None,
        };

        self
    }

    // pub fn thick_start(mut self, thick_start: Position) -> Self {
    //     self.thick_start = Some(thick_start.get() as u64);
    //     self
    // }

    // pub fn thick_end(mut self, thick_end: Position) -> Self {
    //     self.thick_end = Some(thick_end.get() as u64);
    //     self
    // }

    // pub fn color(mut self, color: Option<&BStr>) -> Self {
    //     self.color = color.map(|c| c.to_string());
    //     self
    // }

    // pub fn block_count(mut self, block_count: Option<u64>) -> Self {
    //     self.block_count = block_count;
    //     self
    // }

    // pub fn block_sizes(mut self, block_sizes: Option<String>) -> Self {
    //     self.block_sizes = block_sizes;
    //     self
    // }

    // pub fn block_starts(mut self, block_starts: Option<String>) -> Self {
    //     self.block_starts = block_starts;
    //     self
    // }
}

impl From<RecordBuf<6>> for BEDRecord {
    fn from(value: RecordBuf<6>) -> Self {
        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.feature_start())
            .end(value.feature_end().unwrap())
            .name(value.name())
            .score(Some(value.score()))
            .strand(value.strand());

        builder.finish()
    }
}

impl From<RecordBuf<5>> for BEDRecord {
    fn from(value: RecordBuf<5>) -> Self {
        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.feature_start())
            .end(value.feature_end().unwrap())
            .name(value.name())
            .score(Some(value.score()));

        builder.finish()
    }
}

impl From<RecordBuf<4>> for BEDRecord {
    fn from(value: RecordBuf<4>) -> Self {
        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.feature_start())
            .end(value.feature_end().unwrap());

        builder.finish()
    }
}

impl From<RecordBuf<3>> for BEDRecord {
    fn from(value: RecordBuf<3>) -> Self {
        let builder = BEDRecordBuilder::new()
            .reference_sequence_name(value.reference_sequence_name().to_string())
            .start(value.feature_start())
            .end(value.feature_end().unwrap());

        builder.finish()
    }
}
