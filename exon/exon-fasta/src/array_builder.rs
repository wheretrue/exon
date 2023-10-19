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
    error::ArrowError,
};
use noodles::fasta::Record;

pub struct FASTAArrayBuilder {
    names: GenericStringBuilder<i32>,
    descriptions: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
}

impl FASTAArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            descriptions: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            sequences: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        self.names.append_value(record.name());
        self.descriptions.append_option(record.description());

        let sequence_str = std::str::from_utf8(record.sequence().as_ref()).map_err(|e| {
            ArrowError::ExternalError(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })?;

        self.sequences.append_value(sequence_str);

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let descriptions = self.descriptions.finish();
        let sequences = self.sequences.finish();

        vec![Arc::new(names), Arc::new(descriptions), Arc::new(sequences)]
    }
}
