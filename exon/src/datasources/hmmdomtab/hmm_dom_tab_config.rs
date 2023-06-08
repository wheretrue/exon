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
    csv::{reader::Decoder, ReaderBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use object_store::ObjectStore;

use crate::datasources::DEFAULT_BATCH_SIZE;

/// Configuration for a HMMDomTab data source.
pub struct HMMDomTabConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,
    /// The schema of the HMMDomTab file. This is static.
    pub file_schema: SchemaRef,
    /// The object store to use for reading HMMDomTab files.
    pub object_store: Arc<dyn ObjectStore>,
    /// The decoder to use for reading HMMDomTab files.
    pub decoder: Decoder,
    /// The projection to use for reading HMMDomTab files.
    pub projection: Option<Vec<usize>>,
}

impl HMMDomTabConfig {
    /// Create a new HMMDomTab configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            object_store,
            file_schema: schema().into(),
            batch_size: DEFAULT_BATCH_SIZE,
            decoder: build_hmm_dom_tab_decoder(),
            projection: None,
        }
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

/// Build a decoder for the HMMDomTab file format.
pub fn build_hmm_dom_tab_decoder() -> Decoder {
    let builder = ReaderBuilder::new(schema().into());

    

    builder
        .with_delimiter(b'\t')
        .has_header(false)
        .build_decoder()
}

pub fn schema() -> Schema {
    Schema::new(vec![
        Field::new("target_name", DataType::Utf8, false),
        Field::new("target_accession", DataType::Utf8, false),
        Field::new("tlen", DataType::Int64, false),
        Field::new("query_name", DataType::Utf8, false),
        Field::new("accession", DataType::Utf8, false),
        Field::new("qlen", DataType::Int64, false),
        Field::new("evalue", DataType::Float64, false),
        Field::new("sequence_score", DataType::Float64, false),
        Field::new("bias", DataType::Float64, false),
        Field::new("domain_number", DataType::Int64, false),
        Field::new("ndom", DataType::Int64, false),
        Field::new("conditional_evalue", DataType::Float64, false),
        Field::new("independent_evalue", DataType::Float64, false),
        Field::new("domain_score", DataType::Float64, false),
        Field::new("domain_bias", DataType::Float64, false),
        Field::new("hmm_from", DataType::Int64, false),
        Field::new("hmm_to", DataType::Int64, false),
        Field::new("ali_from", DataType::Int64, false),
        Field::new("ali_to", DataType::Int64, false),
        Field::new("env_from", DataType::Int64, false),
        Field::new("env_to", DataType::Int64, false),
        Field::new("accuracy", DataType::Float64, false),
        Field::new("description", DataType::Utf8, false),
    ])
}
