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
    array::{ArrayBuilder, ArrayRef, GenericStringBuilder, Int32Builder},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
use noodles::sam::{alignment::Record, Header};

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("flag", DataType::Int32, false),
        Field::new("reference", DataType::Utf8, true),
        Field::new("start", DataType::Int32, true),
        Field::new("end", DataType::Int32, true),
        Field::new("mapping_quality", DataType::Utf8, true),
        Field::new("cigar", DataType::Utf8, false),
        Field::new("mate_reference", DataType::Utf8, true),
        Field::new("sequence", DataType::Utf8, false),
        Field::new("quality_score", DataType::Utf8, false),
    ])
}

/// Builds an vector of arrays from a SAM file.
pub struct SAMArrayBuilder {
    names: GenericStringBuilder<i32>,
    flags: Int32Builder,
    references: GenericStringBuilder<i32>,
    starts: Int32Builder,
    ends: Int32Builder,
    mapping_qualities: GenericStringBuilder<i32>,
    cigar: GenericStringBuilder<i32>,
    mate_references: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
    quality_scores: GenericStringBuilder<i32>,

    header: Header,
}

impl SAMArrayBuilder {
    /// Creates a new SAM array builder.
    pub fn create(header: Header) -> Self {
        Self {
            names: GenericStringBuilder::<i32>::new(),
            flags: Int32Builder::new(),
            references: GenericStringBuilder::<i32>::new(),
            starts: Int32Builder::new(),
            ends: Int32Builder::new(),
            mapping_qualities: GenericStringBuilder::<i32>::new(),
            cigar: GenericStringBuilder::<i32>::new(),
            mate_references: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            quality_scores: GenericStringBuilder::<i32>::new(),

            header,
        }
    }

    /// Returns the number of records in the builder.
    pub fn len(&self) -> usize {
        self.quality_scores.len()
    }

    /// Returns whether the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a record to the builder.
    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        self.names.append_option(record.read_name());

        let flag_bits = record.flags().bits();
        self.flags.append_value(flag_bits as i32);

        let reference_name = match record.reference_sequence(&self.header) {
            Some(Ok((name, _))) => Some(name.as_str()),
            Some(Err(_)) => None,
            None => None,
        };
        self.references.append_option(reference_name);

        self.starts
            .append_option(record.alignment_start().map(|v| v.get() as i32));

        self.ends
            .append_option(record.alignment_end().map(|v| v.get() as i32));

        self.mapping_qualities
            .append_option(record.mapping_quality().map(|v| v.get().to_string()));

        let cigar_string = record.cigar().to_string();
        self.cigar.append_value(cigar_string.as_str());

        let mate_reference_name = match record.mate_reference_sequence(&self.header) {
            Some(Ok((name, _))) => Some(name.as_str()),
            Some(Err(_)) => None,
            None => None,
        };
        self.mate_references.append_option(mate_reference_name);

        let sequence_string = record.sequence().to_string();
        self.sequences.append_value(sequence_string.as_str());

        let quality_scores = record.quality_scores();
        self.quality_scores
            .append_value(quality_scores.to_string().as_str());

        Ok(())
    }

    /// Finishes the builder and returns an vector of arrays.
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let flags = self.flags.finish();
        let references = self.references.finish();
        let starts = self.starts.finish();
        let ends = self.ends.finish();
        let mapping_qualities = self.mapping_qualities.finish();
        let cigar = self.cigar.finish();
        let mate_references = self.mate_references.finish();
        let sequences = self.sequences.finish();
        let quality_scores = self.quality_scores.finish();

        vec![
            Arc::new(names),
            Arc::new(flags),
            Arc::new(references),
            Arc::new(starts),
            Arc::new(ends),
            Arc::new(mapping_qualities),
            Arc::new(cigar),
            Arc::new(mate_references),
            Arc::new(sequences),
            Arc::new(quality_scores),
        ]
    }
}
