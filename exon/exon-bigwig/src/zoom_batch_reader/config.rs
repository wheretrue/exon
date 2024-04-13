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

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use exon_common::DEFAULT_BATCH_SIZE;
use object_store::ObjectStore;

/// Configuration for a BigWig datasource.
#[derive(Debug)]
pub struct BigWigZoomConfig {
    /// The number of records to read at a time.
    pub batch_size: usize,

    /// The schema of the BigWig file.
    pub file_schema: SchemaRef,

    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,

    /// The interval to read.
    pub interval: Option<String>,

    /// The reduction to apply.
    pub reduction_level: u32,
}

impl BigWigZoomConfig {
    /// Create a new BigWig configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        let file_schema = Schema::new(Fields::from_iter(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int32, false),
            Field::new("end", DataType::Int32, false),
            Field::new("total_items", DataType::Int32, false),
            Field::new("bases_covered", DataType::Int32, false),
            Field::new("max_value", DataType::Float64, false),
            Field::new("min_value", DataType::Float64, false),
            Field::new("sum_squares", DataType::Float64, false),
            Field::new("sum", DataType::Float64, false),
        ]));

        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            object_store,
            file_schema: Arc::new(file_schema),
            projection: None,
            interval: None,
            reduction_level: 400,
        }
    }

    /// Get the reduction level.
    pub fn reduction_level(&self) -> u32 {
        self.reduction_level
    }

    /// Get the interval.
    pub fn interval(&self) -> Option<&str> {
        self.interval.as_deref()
    }

    /// Set the reduction level.
    pub fn with_reduction_level(mut self, reduction_level: u32) -> Self {
        self.reduction_level = reduction_level;
        self
    }

    /// Set the read type to interval.
    pub fn with_interval(mut self, interval: String) -> Self {
        self.interval = Some(interval);
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
