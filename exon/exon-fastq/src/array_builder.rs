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

use arrow::array::{ArrayRef, GenericStringBuilder};
use exon_common::{ExonArrayBuilder, DEFAULT_BATCH_SIZE};
use noodles::fastq::Record;

use crate::error::{ExonFastqError, ExonFastqResult};

/// A FASTQ record array builder.
pub struct FASTQArrayBuilder {
    /// A builder for the names of the records.
    names: GenericStringBuilder<i32>,
    /// A builder for the descriptions of the records.
    descriptions: GenericStringBuilder<i32>,
    /// A builder for the sequences of the records.
    sequences: GenericStringBuilder<i32>,
    /// A builder for the quality scores of the records.
    quality_scores: GenericStringBuilder<i32>,
    /// The projection of the fields.
    projection: Vec<usize>,
    /// The number of rows.
    rows: usize,
}

impl FASTQArrayBuilder {
    pub fn with_capacity(capacity: usize, projection: Vec<usize>) -> Self {
        Self {
            names: GenericStringBuilder::<i32>::with_capacity(
                capacity,
                capacity * DEFAULT_BATCH_SIZE,
            ),
            descriptions: GenericStringBuilder::<i32>::with_capacity(
                capacity,
                capacity * DEFAULT_BATCH_SIZE,
            ),
            sequences: GenericStringBuilder::<i32>::with_capacity(
                capacity,
                capacity * DEFAULT_BATCH_SIZE,
            ),
            quality_scores: GenericStringBuilder::<i32>::with_capacity(
                capacity,
                capacity * DEFAULT_BATCH_SIZE,
            ),
            projection,
            rows: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    /// Appends a record.
    pub fn append(&mut self, record: &Record) -> ExonFastqResult<()> {
        self.rows += 1;
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let name = std::str::from_utf8(record.name())?;
                    self.names.append_value(name);
                }
                1 => {
                    let desc = record.description();
                    if desc.is_empty() {
                        self.descriptions.append_null();
                    } else {
                        let desc_str = std::str::from_utf8(desc)?;
                        self.descriptions.append_value(desc_str);
                    }
                }
                2 => {
                    let record_sequence = record.sequence();
                    let sequence = std::str::from_utf8(record_sequence)?;
                    self.sequences.append_value(sequence);
                }
                3 => {
                    let record_quality = record.quality_scores();
                    let quality = std::str::from_utf8(record_quality)?;
                    self.quality_scores.append_value(quality);
                }
                _ => {
                    return Err(ExonFastqError::InvalidColumnIndex(*col_idx));
                }
            }
        }

        Ok(())
    }

    /// Builds a record array.
    pub fn finish(&mut self) -> ExonFastqResult<Vec<ArrayRef>> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.projection.len());

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => arrays.push(Arc::new(self.names.finish())),
                1 => arrays.push(Arc::new(self.descriptions.finish())),
                2 => arrays.push(Arc::new(self.sequences.finish())),
                3 => arrays.push(Arc::new(self.quality_scores.finish())),
                c => {
                    return Err(ExonFastqError::InvalidColumnIndex(*c));
                }
            }
        }

        Ok(arrays)
    }
}

impl ExonArrayBuilder for FASTQArrayBuilder {
    fn len(&self) -> usize {
        self.len()
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        self.finish().unwrap()
    }
}
