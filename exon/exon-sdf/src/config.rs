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

use arrow::datatypes::SchemaRef;

/// Configuration for a SDF data source.
#[derive(Debug)]
pub struct SDFConfig {
    pub batch_size: usize,
    pub file_schema: SchemaRef,
}

impl SDFConfig {
    pub fn new(batch_size: usize, file_schema: SchemaRef) -> Self {
        SDFConfig {
            batch_size,
            file_schema,
        }
    }
}
