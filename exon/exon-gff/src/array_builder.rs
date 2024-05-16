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
    array::{
        ArrayRef, Float32Builder, GenericListBuilder, GenericStringBuilder, Int64Builder,
        MapBuilder,
    },
    datatypes::SchemaRef,
    error::ArrowError,
};
use exon_common::ExonArrayBuilder;
use noodles_gff::{lazy::Record, record::attributes::field::Value};

pub struct GFFArrayBuilder {
    seqnames: GenericStringBuilder<i32>,
    sources: GenericStringBuilder<i32>,
    feature_types: GenericStringBuilder<i32>,
    starts: Int64Builder,
    ends: Int64Builder,
    scores: Float32Builder,
    strands: GenericStringBuilder<i32>,
    phases: GenericStringBuilder<i32>,
    attributes:
        MapBuilder<GenericStringBuilder<i32>, GenericListBuilder<i32, GenericStringBuilder<i32>>>,

    projection: Vec<usize>,
    rows: usize,
}

impl GFFArrayBuilder {
    pub fn new(schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let projection = match projection {
            Some(projection) => projection,
            None => (0..schema.fields().len()).collect(),
        };

        Self {
            seqnames: GenericStringBuilder::<i32>::new(),
            sources: GenericStringBuilder::<i32>::new(),
            feature_types: GenericStringBuilder::<i32>::new(),
            starts: Int64Builder::new(),
            ends: Int64Builder::new(),
            scores: Float32Builder::new(),
            strands: GenericStringBuilder::<i32>::new(),
            phases: GenericStringBuilder::<i32>::new(),
            attributes: MapBuilder::new(
                None,
                GenericStringBuilder::<i32>::new(),
                GenericListBuilder::<i32, GenericStringBuilder<i32>>::new(GenericStringBuilder::<
                    i32,
                >::new()),
            ),
            rows: 0,
            projection,
        }
    }

    /// Returns the number of records in the array builder.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Returns whether the array builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => self.seqnames.append_value(record.reference_sequence_name()),
                1 => self.sources.append_value(record.source()),
                2 => self.feature_types.append_value(record.ty()),
                3 => {
                    let start_pos = noodles::core::Position::try_from(record.start())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                    self.starts.append_value(start_pos.get() as i64)
                }
                4 => {
                    let end_pos = noodles::core::Position::try_from(record.end())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    self.ends.append_value(end_pos.get() as i64)
                }
                5 => {
                    let score = record.score();

                    if score.is_empty() || score == "." {
                        self.scores.append_null();
                    } else {
                        let score_f32 = score
                            .parse::<f32>()
                            .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                        self.scores.append_value(score_f32);
                    }
                }
                6 => {
                    let strand = record.strand();

                    if strand.as_ref() == "" || strand.as_ref() == "." {
                        self.strands.append_null();
                    } else {
                        self.strands.append_value(strand);
                    }
                }
                7 => {
                    let phase = record.phase();

                    if phase.is_empty() || phase == "." {
                        self.phases.append_null();
                    } else {
                        self.phases.append_value(phase);
                    }
                }
                8 => {
                    for resp in record.attributes().iter() {
                        let (key, v) = resp?;

                        self.attributes.keys().append_value(key);

                        let value = Value::from_str(v)
                            .map_err(|e| ArrowError::from_external_error(Box::new(e)))?;

                        match value {
                            Value::String(value) => {
                                let values = self.attributes.values();
                                let list_values = values.values();

                                list_values.append_value(value.as_str());
                                values.append(true);
                            }
                            Value::Array(attr_values) => {
                                let values = self.attributes.values();
                                let list_values = values.values();

                                for value in attr_values.iter() {
                                    list_values.append_value(value.as_str());
                                }

                                values.append(true);
                            }
                        }
                    }

                    self.attributes.append(true)?;
                }
                _ => {
                    return Err(ArrowError::ExternalError(
                        "Unexpected number of columns in projections".into(),
                    ))
                }
            }
        }

        self.rows += 1;
        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => arrays.push(Arc::new(self.seqnames.finish())),
                1 => arrays.push(Arc::new(self.sources.finish())),
                2 => arrays.push(Arc::new(self.feature_types.finish())),
                3 => arrays.push(Arc::new(self.starts.finish())),
                4 => arrays.push(Arc::new(self.ends.finish())),
                5 => arrays.push(Arc::new(self.scores.finish())),
                6 => arrays.push(Arc::new(self.strands.finish())),
                7 => arrays.push(Arc::new(self.phases.finish())),
                8 => arrays.push(Arc::new(self.attributes.finish())),
                _ => panic!("Invalid col_idx for GFF ({})", col_idx),
            }
        }

        arrays
    }
}

impl ExonArrayBuilder for GFFArrayBuilder {
    /// Finishes building the internal data structures and returns the built arrays.
    fn finish(&mut self) -> Vec<ArrayRef> {
        self.finish()
    }

    /// Returns the number of elements in the array.
    fn len(&self) -> usize {
        self.rows
    }
}
