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

use arrow::datatypes::SchemaRef;
use object_store::ObjectStore;

/// Configuration for a SDF data source.
#[derive(Debug)]
pub struct SDFConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the file.
    pub file_schema: SchemaRef,

    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,
}

impl SDFConfig {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        batch_size: usize,
        file_schema: SchemaRef,
    ) -> Self {
        SDFConfig {
            object_store,
            batch_size,
            file_schema,
        }
    }
}
