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

use arrow::array::{ArrayBuilder, ArrayRef, Float32Builder};
/// A builder for an FCS array
pub struct FCSArrayBuilder {
    /// an array of f32 builders
    builders: Vec<Float32Builder>,
}

impl FCSArrayBuilder {
    /// Create a new FCSArrayBuilder
    ///
    /// # Arguments
    ///
    /// * `n_fields` - the number of fields in the array
    pub fn create(n_fields: usize) -> Self {
        let mut builders = Vec::with_capacity(n_fields);
        for _ in 0..n_fields {
            builders.push(Float32Builder::new());
        }
        Self { builders }
    }

    /// Get the length of the array
    pub fn len(&self) -> usize {
        if self.builders.len() == 0 {
            return 0;
        }

        self.builders[0].len()
    }

    /// Append a record to the array
    ///
    /// # Arguments
    ///
    /// * `record` - a vector of f32 values
    ///
    /// # Returns
    ///
    /// * `Result<(), ArrowError>` - an error if the record is not the same length as the number of fields
    pub fn append(&mut self, record: Vec<f32>) {
        // Zip the record with the builders and append the values
        for (value, builder) in record.iter().zip(self.builders.iter_mut()) {
            builder.append_value(*value);
        }
    }

    /// Finish the array
    ///
    /// # Returns
    ///
    /// * `Vec<ArrayRef>` - a vector of ArrayRefs
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        self.builders
            .iter_mut()
            .map(|builder| Arc::new(builder.finish()) as ArrayRef)
            .collect::<Vec<_>>()
    }
}
