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
use exon_common::{TableSchema, DEFAULT_BATCH_SIZE};
use noodles::core::Region;
use object_store::ObjectStore;

pub struct SchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        let file_fields = vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("start", DataType::Int32, false),
            Field::new("end", DataType::Int32, false),
            Field::new("value", DataType::Float32, false),
        ];

        Self {
            file_fields,
            partition_fields: vec![],
        }
    }
}

impl SchemaBuilder {
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

#[derive(Debug)]
pub enum ValueReadType {
    Interval(Region),
    Scan,
}

/// Configuration for a BigWig datasource.
#[derive(Debug)]
pub struct BigWigValueConfig {
    /// The number of records to read at a time.
    pub batch_size: usize,

    /// The schema of the BigWig file.
    pub file_schema: SchemaRef,

    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,

    /// The type of read to perform.
    pub read_type: ValueReadType,
}

impl BigWigValueConfig {
    /// Create a new BigWig configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        let file_schema = Schema::new(Fields::from_iter(vec![
            Field::new("chrom", DataType::Utf8, false),
            Field::new("start", DataType::Int32, false),
            Field::new("end", DataType::Int32, false),
            Field::new("value", DataType::Float32, false),
        ]));

        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            object_store,
            file_schema: Arc::new(file_schema),
            projection: None,
            read_type: ValueReadType::Scan,
        }
    }

    /// Create a new BigWig configuration.
    pub fn new_with_schema(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            object_store,
            file_schema,
            projection: None,
            read_type: ValueReadType::Scan,
        }
    }

    /// Set the read type to interval.
    pub fn with_some_interval(mut self, interval: Option<Region>) -> Self {
        if let Some(interval) = interval {
            self.read_type = ValueReadType::Interval(interval);
        } else {
            self.read_type = ValueReadType::Scan;
        }

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
