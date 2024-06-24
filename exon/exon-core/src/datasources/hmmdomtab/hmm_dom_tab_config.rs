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

use arrow::{
    csv::{reader::Decoder, ReaderBuilder},
    datatypes::SchemaRef,
};
use exon_common::DEFAULT_BATCH_SIZE;
use object_store::ObjectStore;

/// Configuration for a HMMDomTab data source.
pub struct HMMDomTabConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,
    /// The schema of the HMMDomTab file. This is static.
    pub file_schema: SchemaRef,
    /// The object store to use for reading HMMDomTab files.
    pub object_store: Arc<dyn ObjectStore>,
    /// The projection to use for reading HMMDomTab files.
    pub projection: Option<Vec<usize>>,
}

impl HMMDomTabConfig {
    /// Create a new HMMDomTab configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            object_store,
            file_schema,
            batch_size: DEFAULT_BATCH_SIZE,
            projection: None,
        }
    }

    /// Build a decoder for this configuration.
    pub fn build_decoder(&self) -> Decoder {
        let builder = ReaderBuilder::new(self.file_schema.clone())
            .with_header(false)
            .with_delimiter(b'\t')
            .with_batch_size(self.batch_size);

        builder.build_decoder()
    }

    /// Set the file schema.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the projection.
    pub fn with_some_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }
}
