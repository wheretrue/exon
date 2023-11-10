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

use datafusion::error::Result;

#[derive(Debug, Clone)]
/// An object that holds the schema for a table and the projection of the file schema to the table schema.
pub struct TableSchema {
    schema: SchemaRef,
    file_projection: Vec<usize>,
}

impl TableSchema {
    /// Create a new table schema
    pub fn new(schema: SchemaRef, file_projection: Vec<usize>) -> Self {
        Self {
            schema,
            file_projection,
        }
    }

    /// Get the schema for the underlying file
    pub fn file_schema(&self) -> Result<SchemaRef> {
        let file_schema = &self.schema.project(&self.file_projection)?;
        Ok(Arc::new(file_schema.clone()))
    }

    /// Get the schema for the table
    pub fn table_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
