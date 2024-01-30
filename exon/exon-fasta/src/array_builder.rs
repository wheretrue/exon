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
    array::{ArrayBuilder, ArrayRef, GenericStringBuilder},
    datatypes::{DataType, SchemaRef},
    error::ArrowError,
};
use noodles::fasta::Record;

use crate::ExonFastaError;

pub struct FASTAArrayBuilder {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: SequenceBuilder,
}

enum SequenceBuilder {
    Utf8(GenericStringBuilder<i32>),
    LargeUtf8(GenericStringBuilder<i64>),
}

impl SequenceBuilder {
    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(ref mut builder) => Arc::new(builder.finish()),
            Self::LargeUtf8(ref mut builder) => Arc::new(builder.finish()),
        }
    }
}

impl FASTAArrayBuilder {
    /// Create a new FASTA array builder.
    pub fn create(schema: SchemaRef, capacity: usize) -> Result<Self, ArrowError> {
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

        Ok(Self {
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            descriptions: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            sequences: sequence_builder,
        })
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ExonFastaError> {
        let name = std::str::from_utf8(record.name())?;
        self.names.append_value(name);

        if let Some(description) = record.description() {
            let description = std::str::from_utf8(description)?;
            self.descriptions.append_value(description);
        } else {
            self.descriptions.append_null();
        }

        let sequence_str = std::str::from_utf8(record.sequence().as_ref())?;

        match &mut self.sequences {
            SequenceBuilder::Utf8(builder) => builder.append_value(sequence_str),
            SequenceBuilder::LargeUtf8(builder) => builder.append_value(sequence_str),
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();

        vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)]
    }
}
