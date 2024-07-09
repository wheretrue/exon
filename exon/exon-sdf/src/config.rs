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

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
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
            projection: None,
        }
    }

    /// Get the projection.
    pub fn projection(&self) -> Vec<usize> {
        self.projection
            .clone()
            .unwrap_or_else(|| (0..self.file_schema.fields().len()).collect())
    }

    /// Get the projected schema.
    pub fn projected_schema(&self) -> arrow::error::Result<SchemaRef> {
        let schema = self.file_schema.project(&self.projection())?;

        Ok(Arc::new(schema))
    }

    /// Create a new SDF configuration with a given projection.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        // Only include fields that are in the file schema.
        // TODO: make this cleaner, i.e. projection should probably come
        // pre-filtered.
        let file_projection = projection
            .iter()
            .filter(|f| **f < self.file_schema.fields().len())
            .cloned()
            .collect::<Vec<_>>();

        self.projection = Some(file_projection);
        self
    }
}
