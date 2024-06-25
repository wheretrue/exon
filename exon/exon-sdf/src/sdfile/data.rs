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

#[derive(Debug)]
pub struct Datum {
    header: String,
    data: String,
}

impl Datum {
    pub fn new(header: String, data: String) -> Self {
        Datum { header, data }
    }

    pub fn header(&self) -> &str {
        &self.header
    }

    pub fn data(&self) -> &str {
        &self.data
    }
}

#[derive(Debug, Default)]
pub struct Data {
    data: Vec<Datum>,
}

impl Data {
    pub fn push(&mut self, header: String, data: String) {
        self.data.push(Datum::new(header, data));
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}
