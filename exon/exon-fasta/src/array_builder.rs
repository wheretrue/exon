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

use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{ArrayRef, GenericListBuilder, GenericStringBuilder, Int32Builder},
    datatypes::SchemaRef,
    error::ArrowError,
};
use exon_common::ExonArrayBuilder;
use noodles::fasta::record::Definition;

use crate::{ExonFastaError, SequenceDataType};

pub struct FASTAArrayBuilder {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: SequenceBuilder,
    projection: Vec<usize>,
    append_name: bool,
    append_description: bool,
    append_sequence: bool,
    rows: usize,
}

pub enum SequenceBuilder {
    Utf8(GenericStringBuilder<i32>),
    LargeUtf8(GenericStringBuilder<i64>),
    IntegerEncodeDNA(GenericListBuilder<i32, Int32Builder>),
    IntegerEncodeProtein(GenericListBuilder<i32, Int32Builder>),
}

impl SequenceBuilder {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(ref mut builder) => Arc::new(builder.finish()),
            Self::LargeUtf8(ref mut builder) => Arc::new(builder.finish()),
            Self::IntegerEncodeProtein(ref mut builder) => Arc::new(builder.finish()),
            Self::IntegerEncodeDNA(ref mut builder) => Arc::new(builder.finish()),
        }
    }
}

impl FASTAArrayBuilder {
    /// Create a new FASTA array builder.
    pub fn create(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        capacity: usize,
        sequence_data_type: &SequenceDataType,
    ) -> Result<Self, ArrowError> {
        let sequence_builder = match sequence_data_type {
            SequenceDataType::Utf8 => SequenceBuilder::Utf8(
                GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            ),
            SequenceDataType::LargeUtf8 => SequenceBuilder::LargeUtf8(
                GenericStringBuilder::<i64>::with_capacity(capacity, capacity),
            ),
            SequenceDataType::IntegerEncodeProtein => SequenceBuilder::IntegerEncodeProtein(
                GenericListBuilder::<i32, Int32Builder>::with_capacity(
                    Int32Builder::with_capacity(60),
                    capacity,
                ),
            ),
            SequenceDataType::IntegerEncodeDNA => SequenceBuilder::IntegerEncodeDNA(
                GenericListBuilder::<i32, Int32Builder>::with_capacity(
                    Int32Builder::with_capacity(60),
                    capacity,
                ),
            ),
        };

        let projection = match projection {
            Some(projection) => projection,
            None => (0..schema.fields().len()).collect(),
        };

        let append_name = projection.contains(&0);
        let append_description = projection.contains(&1);
        let append_sequence = projection.contains(&2);

        Ok(Self {
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            descriptions: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            sequences: sequence_builder,
            projection,
            rows: 0,
            append_sequence,
            append_name,
            append_description,
        })
    }

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, definition: &str, sequence: &[u8]) -> Result<(), ArrowError> {
        if self.append_name || self.append_description {
            let definition = Definition::from_str(definition)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

            if self.append_name {
                let name = std::str::from_utf8(definition.name())?;
                self.names.append_value(name);
            }

            if self.append_description {
                if let Some(description) = definition.description() {
                    let description = std::str::from_utf8(description)?;
                    self.descriptions.append_value(description);
                } else {
                    self.descriptions.append_null();
                }
            }
        }

        if self.append_sequence {
            match &mut self.sequences {
                SequenceBuilder::Utf8(ref mut builder) => {
                    let sequence = std::str::from_utf8(sequence)?;
                    builder.append_value(sequence);
                }
                SequenceBuilder::LargeUtf8(ref mut builder) => {
                    let sequence = std::str::from_utf8(sequence)?;
                    builder.append_value(sequence);
                }
                SequenceBuilder::IntegerEncodeProtein(ref mut builder) => {
                    let values = builder.values();

                    for aa in sequence {
                        let aa = match aa {
                            b'A' => 1,
                            b'B' => 2,
                            b'C' => 3,
                            b'D' => 4,
                            b'E' => 5,
                            b'F' => 6,
                            b'G' => 7,
                            b'H' => 8,
                            b'I' => 9,
                            b'K' => 10,
                            b'L' => 11,
                            b'M' => 12,
                            b'N' => 13,
                            b'O' => 14,
                            b'P' => 15,
                            b'Q' => 16,
                            b'R' => 17,
                            b'S' => 18,
                            b'T' => 19,
                            b'U' => 20,
                            b'V' => 21,
                            b'W' => 22,
                            b'Y' => 23,
                            b'X' => 24,
                            b'Z' => 25,
                            _ => {
                                return Err(ExonFastaError::InvalidAminoAcid(*aa).into());
                            }
                        };

                        values.append_value(aa);
                    }

                    builder.append(true);
                }
                SequenceBuilder::IntegerEncodeDNA(ref mut builder) => {
                    let values = builder.values();

                    // Convert the DNA sequence to one-hot encoding, use A => 1, C => 2, G => 3, T => 4, N => 5
                    // error for non-ACGTN characters
                    for nt in sequence {
                        let nt = match nt {
                            b'A' => 1,
                            b'C' => 2,
                            b'G' => 3,
                            b'T' => 4,
                            b'N' => 5,
                            _ => {
                                return Err(ExonFastaError::InvalidNucleotide(*nt).into());
                            }
                        };

                        values.append_value(nt);
                    }

                    builder.append(true);
                }
            }
        }

        self.rows += 1;
        Ok(())
    }

    fn finish_inner(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.projection.len());

        if self.append_name {
            arrays.push(Arc::new(self.names.finish()));
        }

        if self.append_description {
            arrays.push(Arc::new(self.descriptions.finish()));
        }

        if self.append_sequence {
            arrays.push(self.sequences.finish());
        }

        arrays
    }
}

impl ExonArrayBuilder for FASTAArrayBuilder {
    fn finish(&mut self) -> Vec<ArrayRef> {
        self.finish_inner()
    }

    fn len(&self) -> usize {
        self.len()
    }
}
