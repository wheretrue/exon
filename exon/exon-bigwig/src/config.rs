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

use arrow::datatypes::SchemaRef;
use exon_common::DEFAULT_BATCH_SIZE;
use object_store::ObjectStore;

#[derive(Debug)]
pub enum BigWigReadType {
    Zoom((u32, String)),
    Interval(String),
    Scan,
}

/// Configuration for a BigWig datasource.
#[derive(Debug)]
pub struct BigWigConfig {
    /// The number of records to read at a time.
    pub batch_size: usize,

    /// The schema of the BigWig file.
    pub file_schema: SchemaRef,

    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,

    /// The type of read to perform.
    pub read_type: BigWigReadType,
}

impl BigWigConfig {
    /// Create a new BigWig configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            object_store,
            file_schema,
            projection: None,
            read_type: BigWigReadType::Scan,
        }
    }

    /// Set the read type to zoom.
    pub fn with_zoom(mut self, zoom: (u32, String)) -> Self {
        self.read_type = BigWigReadType::Zoom(zoom);
        self
    }

    /// Set the read type to interval.
    pub fn with_interval(mut self, interval: String) -> Self {
        self.read_type = BigWigReadType::Interval(interval);
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the projection.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the projection from an optional vector.
    pub fn with_some_projection(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }
}
