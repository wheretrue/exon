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
    array::{ArrayBuilder, ArrayRef, GenericListBuilder, GenericStringBuilder, Int32Builder},
    datatypes::{DataType, SchemaRef},
    error::ArrowError,
};
use exon_common::ExonArrayBuilder;
use noodles::fasta::record::Definition;

pub struct FASTAArrayBuilder {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: SequenceBuilder,
    projection: Vec<usize>,
    append_sequence: bool,
    rows: usize,
}

pub enum SequenceBuilder {
    Utf8(GenericStringBuilder<i32>),
    LargeUtf8(GenericStringBuilder<i64>),
    OneHotProtein(GenericListBuilder<i32, Int32Builder>),
}

impl SequenceBuilder {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(ref mut builder) => Arc::new(builder.finish()),
            Self::LargeUtf8(ref mut builder) => Arc::new(builder.finish()),
            Self::OneHotProtein(ref mut builder) => Arc::new(builder.finish()),
        }
    }
}

impl FASTAArrayBuilder {
    /// Create a new FASTA array builder.
    pub fn create(
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        capacity: usize,
    ) -> Result<Self, ArrowError> {
        let sequence_field = schema.field_with_name("sequence")?;

        let sequence_builder = match sequence_field.data_type() {
            DataType::Utf8 => SequenceBuilder::Utf8(GenericStringBuilder::<i32>::with_capacity(
                capacity, capacity,
            )),
            DataType::LargeUtf8 => SequenceBuilder::LargeUtf8(
                GenericStringBuilder::<i64>::with_capacity(capacity, capacity),
            ),
            _ => {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Unsupported sequence data type: {:?}",
                    sequence_field.data_type()
                )))
            }
        };

        let projection = match projection {
            Some(projection) => projection,
            None => (0..schema.fields().len()).collect(),
        };

        let append_sequence = true;

        Ok(Self {
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            descriptions: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            sequences: sequence_builder,
            projection,
            rows: 0,
            append_sequence,
        })
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, definition: &str, sequence: &[u8]) -> Result<(), ArrowError> {
        let definition =
            Definition::from_str(definition).map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

        let name = std::str::from_utf8(definition.name())?;
        self.names.append_value(name);

        if let Some(description) = definition.description() {
            let description = std::str::from_utf8(description)?;
            self.descriptions.append_value(description);
        } else {
            self.descriptions.append_null();
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
                SequenceBuilder::OneHotProtein(ref mut builder) => {
                    let values = builder.values();

                    for aa in sequence {
                        let aa = *aa as i32;
                        values.append_value(aa);
                    }
                }
            }
        }

        self.rows += 1;
        Ok(())
    }

    fn finish_inner(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.projection.len());

        arrays.push(Arc::new(self.names.finish()));
        arrays.push(Arc::new(self.descriptions.finish()));

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
