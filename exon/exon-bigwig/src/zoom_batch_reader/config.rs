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
    datatypes::{DataType, Field, Schema, SchemaRef},
    error::Result as ArrowResult,
};
use exon_common::{TableSchema, DEFAULT_BATCH_SIZE};
use noodles::core::Region;
use object_store::ObjectStore;

pub struct BigWigSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

impl Default for BigWigSchemaBuilder {
    fn default() -> Self {
        let file_fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("start", DataType::Int32, false),
            Field::new("end", DataType::Int32, false),
            Field::new("total_items", DataType::Int32, false),
            Field::new("bases_covered", DataType::Int32, false),
            Field::new("max_value", DataType::Float64, false),
            Field::new("min_value", DataType::Float64, false),
            Field::new("sum_squares", DataType::Float64, false),
            Field::new("sum", DataType::Float64, false),
        ];

        Self {
            file_fields,
            partition_fields: vec![],
        }
    }
}

impl BigWigSchemaBuilder {
    pub fn new(file_fields: Vec<Field>, partition_fields: Vec<Field>) -> Self {
        Self {
            file_fields,
            partition_fields,
        }
    }

    pub fn add_partition_fields(&mut self, fields: Vec<Field>) {
        self.partition_fields.extend(fields);
    }

    /// Returns the schema and the projection indexes for the file's schema
    pub fn build(self) -> TableSchema {
        let mut fields = self.file_fields.clone();
        fields.extend_from_slice(&self.partition_fields);

        let schema = Schema::new(fields);

        let projection = (0..self.file_fields.len()).collect::<Vec<_>>();

        TableSchema::new(Arc::new(schema), projection)
    }
}

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
    pub interval: Option<Region>,

    /// The reduction to apply.
    pub reduction_level: u32,
}

impl BigWigZoomConfig {
    /// Create a new BigWig configuration.
    pub fn new_with_schema(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            object_store,
            file_schema,
            projection: None,
            interval: None,
            reduction_level: 400,
        }
    }

    pub fn new(object_store: Arc<dyn ObjectStore>) -> ArrowResult<Self> {
        let schema = BigWigSchemaBuilder::default().build();
        let file_schema = schema.file_schema()?;

        Ok(Self::new_with_schema(object_store, file_schema))
    }

    /// Get the reduction level.
    pub fn reduction_level(&self) -> u32 {
        self.reduction_level
    }

    /// Get the interval.
    pub fn interval(&self) -> Option<&Region> {
        self.interval.as_ref()
    }

    /// Set the reduction level.
    pub fn with_reduction_level(mut self, reduction_level: u32) -> Self {
        self.reduction_level = reduction_level;
        self
    }

    /// Set the interval.
    pub fn with_interval(mut self, interval: Region) -> Self {
        self.interval = Some(interval);
        self
    }

    /// Set the interval from an optional region.
    pub fn with_some_interval(mut self, interval: Option<Region>) -> Self {
        self.interval = interval;
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
