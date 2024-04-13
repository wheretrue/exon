// Copyright 2024 WHERE TRUE Technologies.
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

use arrow::array::{ArrayRef, Float32Builder, GenericStringBuilder, Int32Builder};
use bigtools::Value;

pub struct ValueArrayBuilder {
    names: GenericStringBuilder<i32>,
    start: Int32Builder,
    end: Int32Builder,
    value: Float32Builder,

    n_rows: usize,
}

impl ValueArrayBuilder {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            names: GenericStringBuilder::<i32>::with_capacity(capacity, capacity),
            start: Int32Builder::with_capacity(capacity),
            end: Int32Builder::with_capacity(capacity),
            value: Float32Builder::with_capacity(capacity),
            n_rows: 0,
        }
    }

    pub fn append(&mut self, name: &str, value: Value) {
        self.names.append_value(name);

        self.start.append_value(value.start as i32);
        self.end.append_value(value.end as i32);

        self.value.append_value(value.value);

        self.n_rows += 1;
    }

    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let names = self.names.finish();
        let start = self.start.finish();
        let end = self.end.finish();
        let value = self.value.finish();

        vec![
            Arc::new(names),
            Arc::new(start),
            Arc::new(end),
            Arc::new(value),
        ]
    }
}
