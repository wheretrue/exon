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
    array::{ArrayRef, GenericListBuilder, GenericStringBuilder, Int32Builder, StructBuilder},
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use itertools::Itertools;
use noodles::sam::header::ReferenceSequences;

const BATCH_SIZE: usize = 8192;

use super::indexed_async_batch_stream::SemiLazyRecord;

/// Builds an vector of arrays from a SAM file.
pub struct BAMArrayBuilder {
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

    tags: GenericListBuilder<i32, StructBuilder>,

    projection: Vec<usize>,

    rows: usize,

    reference_names: Vec<String>,
}

impl BAMArrayBuilder {
    /// Creates a new SAM array builder.
    pub fn create(reference_sequences: Arc<ReferenceSequences>, projection: Vec<usize>) -> Self {
        let tag_field = Field::new("tag", DataType::Utf8, false);
        let value_field = Field::new("value", DataType::Utf8, true);

        let tag = StructBuilder::new(
            Fields::from(vec![tag_field, value_field]),
            vec![
                Box::new(GenericStringBuilder::<i32>::new()),
                Box::new(GenericStringBuilder::<i32>::new()),
            ],
        );

        let reference_names = reference_sequences
            .keys()
            .map(|k| k.to_string())
            .collect::<Vec<_>>();

        let item_capacity = BATCH_SIZE;

        Self {
            names: GenericStringBuilder::<i32>::new(),
            flags: Int32Builder::new(),
            references: GenericStringBuilder::<i32>::with_capacity(
                item_capacity,
                item_capacity * 10,
            ),
            starts: Int32Builder::with_capacity(item_capacity),
            ends: Int32Builder::with_capacity(item_capacity),
            mapping_qualities: GenericStringBuilder::<i32>::new(),
            cigar: GenericStringBuilder::<i32>::new(),
            mate_references: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            quality_scores: GenericStringBuilder::<i32>::new(),

            tags: GenericListBuilder::new(tag),

            projection,

            rows: 0,

            reference_names,
        }
    }

    /// Appends a record to the builder.
    pub(crate) fn append(&mut self, record: &SemiLazyRecord) -> Result<(), ArrowError> {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let sam_read_name: Option<noodles::sam::record::ReadName> =
                        record.record().read_name().map(|v| v.try_into().unwrap());

                    self.names.append_option(sam_read_name);
                }
                1 => {
                    let flag_bits = record.record().flags().bits();
                    self.flags.append_value(flag_bits as i32);
                }
                2 => match record.record().reference_sequence_id()? {
                    Some(reference_sequence_id) => {
                        let reference_name = &self.reference_names[reference_sequence_id];

                        self.references.append_value(reference_name);
                    }
                    None => {
                        self.references.append_null();
                    }
                },
                3 => {
                    self.starts
                        .append_option(record.record().alignment_start()?.map(|v| v.get() as i32));
                }
                4 => {
                    let alignment_end = record.alignment_end().map(|v| v.get() as i32);
                    self.ends.append_option(alignment_end);
                }
                5 => {
                    self.mapping_qualities.append_option(
                        record
                            .record()
                            .mapping_quality()
                            .map(|v| v.get().to_string()),
                    );
                }
                6 => {
                    let cigar = record.cigar();
                    let cigar_string = cigar.iter().map(|c| c.to_string()).join("");

                    self.cigar.append_value(cigar_string.as_str());
                }
                7 => match record.record().mate_reference_sequence_id()? {
                    Some(mate_reference_sequence_id) => {
                        let mate_reference_name = &self.reference_names[mate_reference_sequence_id];

                        self.mate_references.append_value(mate_reference_name);
                    }
                    None => {
                        self.mate_references.append_null();
                    }
                },
                8 => {
                    let sequence = record.record().sequence();
                    let sam_record_sequence: noodles::sam::record::Sequence =
                        sequence.try_into()?;

                    self.sequences.append_value(sam_record_sequence.to_string());
                }
                9 => {
                    let quality_scores = record.record().quality_scores();
                    let sam_record_quality_scores: noodles::sam::record::QualityScores =
                        quality_scores.try_into()?;

                    self.quality_scores
                        .append_value(sam_record_quality_scores.to_string().as_str());
                }
                10 => {
                    let data = record.record().data();
                    let data: noodles::sam::record::Data = data.try_into()?;
                    let tags = data.keys();

                    let tag_struct = self.tags.values();
                    for tag in tags {
                        let tag_value = data.get(&tag).unwrap();

                        let tag_value_string = tag_value.to_string();

                        tag_struct
                            .field_builder::<GenericStringBuilder<i32>>(0)
                            .unwrap()
                            .append_value(tag.to_string());

                        tag_struct
                            .field_builder::<GenericStringBuilder<i32>>(1)
                            .unwrap()
                            .append_value(tag_value_string);

                        tag_struct.append(true);
                    }
                    self.tags.append(true);
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Invalid column index {} for SAM",
                        col_idx
                    )))
                }
            }
        }

        self.rows += 1;

        Ok(())
    }

    /// Finishes the builder and returns an vector of arrays.
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => arrays.push(Arc::new(self.names.finish())),
                1 => arrays.push(Arc::new(self.flags.finish())),
                2 => arrays.push(Arc::new(self.references.finish())),
                3 => arrays.push(Arc::new(self.starts.finish())),
                4 => arrays.push(Arc::new(self.ends.finish())),
                5 => arrays.push(Arc::new(self.mapping_qualities.finish())),
                6 => arrays.push(Arc::new(self.cigar.finish())),
                7 => arrays.push(Arc::new(self.mate_references.finish())),
                8 => arrays.push(Arc::new(self.sequences.finish())),
                9 => arrays.push(Arc::new(self.quality_scores.finish())),
                10 => arrays.push(Arc::new(self.tags.finish())),
                _ => panic!("Invalid column index {} for SAM", col_idx),
            }
        }

        arrays
    }
}
