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
use exon_common::DEFAULT_BATCH_SIZE;
use object_store::ObjectStore;

use crate::ExonBEDResult;

/// Configuration for a BED datasource.
#[derive(Debug)]
pub struct BEDConfig {
    /// The number of records to read at a time.
    pub batch_size: usize,

    /// The schema of the BED file.
    pub file_schema: SchemaRef,

    /// The object store to use.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,

    /// The number of fields of the BED to read.
    pub n_fields: Option<usize>,
}

impl BEDConfig {
    /// Create a new BED configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            batch_size: DEFAULT_BATCH_SIZE,
            object_store,
            file_schema,
            projection: None,
            n_fields: None,
        }
    }

    /// Set the number of fields.
    pub fn with_n_fields(mut self, n_fields: usize) -> Self {
        self.n_fields = Some(n_fields);
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

    /// Get the projected schema.
    pub fn projected_schema(&self) -> ExonBEDResult<SchemaRef> {
        let schema = self.file_schema.project(&self.projection())?;

        Ok(Arc::new(schema))
    }

    /// Return the projection, while accounting for the number of fields.
    pub fn projection(&self) -> Vec<usize> {
        match (&self.projection, &self.n_fields) {
            (Some(projection), Some(n_fields)) => projection
                .iter()
                .filter(|&i| i < n_fields)
                .copied()
                .collect(),
            (Some(projection), None) => projection.clone(),
            (_, Some(n_fields)) => (0..*n_fields).collect(),
            (_, _) => (0..self.file_schema.fields().len()).collect(),
        }
    }
}
