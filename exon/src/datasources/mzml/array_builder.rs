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

use arrow::array::{ArrayBuilder, ArrayRef, GenericStringBuilder};

use super::mzml_reader::types::Spectrum;

pub struct MzMLArrayBuilder {
    id: GenericStringBuilder<i32>,
}

impl MzMLArrayBuilder {
    pub fn new() -> Self {
        Self {
            id: GenericStringBuilder::<i32>::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.id.len()
    }

    pub fn append(&mut self, record: &Spectrum) {
        self.id.append_value(&record.id);
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let id = self.id.finish();

        vec![Arc::new(id)]
    }
}
