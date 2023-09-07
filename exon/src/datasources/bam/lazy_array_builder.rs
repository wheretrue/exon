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
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::ArrowError,
};
use datafusion::common::format;
use noodles::{
    bam::lazy::Record,
    sam::{
        header::{
            record::value::{
                map::{self, ReferenceSequence},
                Map,
            },
            ReferenceSequences,
        },
        record::{QualityScores, ReadName, Sequence},
        Header,
    },
};

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

fn get_reference_sequence(
    reference_sequences: &ReferenceSequences,
    reference_sequence_id: Option<usize>,
) -> Option<std::io::Result<(&map::reference_sequence::Name, &Map<ReferenceSequence>)>> {
    reference_sequence_id.map(|id| {
        reference_sequences.get_index(id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid reference sequence ID",
            )
        })
    })
}

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

    schema: SchemaRef,
    header: Arc<Header>,
    projection: Vec<usize>,
}

impl BAMArrayBuilder {
    pub fn try_new(
        schema: SchemaRef,
        capacity: usize,
        projection: Option<Vec<usize>>,
        header: Arc<Header>,
    ) -> Result<Self, ArrowError> {
        let projection = match projection {
            Some(p) => p,
            None => (0..schema.fields().len()).collect(),
        };

        Ok(Self {
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

            schema,
            header,
            projection,
        })
    }

    pub fn len(&self) -> usize {
        self.quality_scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.quality_scores.is_empty()
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        // TODO: only parse the cigar once if col_idx causes it to be needed (col_idx in {4, ...})

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let read_name = record
                        .read_name()
                        .map(ReadName::try_from)
                        .transpose()?
                        .map(|v| format!("{}", v));

                    self.names.append_option(read_name);
                }
                1 => {
                    let bits = record.flags()?.bits();
                    self.flags.append_value(bits as i32);
                }
                2 => match record.reference_sequence_id()? {
                    Some(s) => {
                        let reference_sequence =
                            get_reference_sequence(self.header.reference_sequences(), Some(s))
                                .transpose()?;

                        match reference_sequence {
                            Some((name, _)) => {
                                self.references.append_value(name.as_str());
                            }
                            None => {
                                self.references.append_null();
                            }
                        }
                    }
                    None => {
                        self.references.append_null();
                    }
                },
                3 => {
                    let starts = record.alignment_start()?.map(|v| v.get() as i32);
                    self.starts.append_option(starts);
                }
                4 => {
                    let cigar = record.cigar();
                    let cigar = noodles::sam::record::Cigar::try_from(cigar)?;

                    let end = record.alignment_start()?.map(|alignment_start| {
                        let end = usize::from(alignment_start) + cigar.alignment_span() - 1;

                        end as i32
                    });

                    self.ends.append_option(end);
                }
                5 => {
                    let mapping_quality = record.mapping_quality();
                    let mapping_quality = mapping_quality.map(|v| v.get().to_string());

                    self.mapping_qualities.append_option(mapping_quality);
                }
                6 => {
                    let cigar = record.cigar();
                    let cigar = noodles::sam::record::Cigar::try_from(cigar)?.to_string();

                    // Make sure schema is not null
                    self.cigar.append_value(cigar.as_str());
                }
                7 => match record.mate_reference_sequence_id()? {
                    Some(s) => {
                        let reference_sequence =
                            get_reference_sequence(self.header.reference_sequences(), Some(s))
                                .transpose()?;

                        match reference_sequence {
                            Some((name, _)) => {
                                self.references.append_value(name.as_str());
                            }
                            None => {
                                self.references.append_null();
                            }
                        }
                    }
                    None => {
                        self.mate_references.append_null();
                    }
                },
                8 => {
                    let sequence = Sequence::try_from(record.sequence())?;
                    self.sequences.append_value(sequence.to_string());
                }
                9 => {
                    let quality_scores = record.quality_scores();
                    let quality_scores = QualityScores::try_from(quality_scores)?;

                    self.quality_scores
                        .append_value(quality_scores.to_string().as_str());
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Invalid column index: {}",
                        col_idx
                    )))
                }
            }
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = vec![];

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let names = self.names.finish();
                    arrays.push(Arc::new(names));
                }
                1 => {
                    let flags = self.flags.finish();
                    arrays.push(Arc::new(flags));
                }
                2 => {
                    let references = self.references.finish();
                    arrays.push(Arc::new(references));
                }
                3 => {
                    let starts = self.starts.finish();
                    arrays.push(Arc::new(starts));
                }
                4 => {
                    let ends = self.ends.finish();
                    arrays.push(Arc::new(ends));
                }
                5 => {
                    let mapping_qualities = self.mapping_qualities.finish();
                    arrays.push(Arc::new(mapping_qualities));
                }
                6 => {
                    let cigar = self.cigar.finish();
                    arrays.push(Arc::new(cigar));
                }
                7 => {
                    let mate_references = self.mate_references.finish();
                    arrays.push(Arc::new(mate_references));
                }
                8 => {
                    let sequences = self.sequences.finish();
                    arrays.push(Arc::new(sequences));
                }
                9 => {
                    let quality_scores = self.quality_scores.finish();
                    arrays.push(Arc::new(quality_scores));
                }
                _ => panic!("Invalid column index: {}", col_idx),
            }
        }

        arrays
    }
}
