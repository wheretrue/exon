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

use arrow::datatypes::{Field, Fields, Schema, SchemaRef};

use datafusion::error::Result;

/// A builder for `TableSchema`.
pub struct TableSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

impl Default for TableSchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TableSchemaBuilder {
    /// Creates a new builder for `TableSchema`.
    pub fn new() -> Self {
        Self {
            file_fields: Vec::new(),
            partition_fields: Vec::new(),
        }
    }

    /// Create a new builder with the passed file fields.
    pub fn new_with_field_fields(file_fields: Vec<Field>) -> Self {
        Self {
            file_fields,
            partition_fields: Vec::new(),
        }
    }

    /// Adds file fields to the `TableSchema`.
    pub fn add_file_fields(mut self, fields: Vec<Field>) -> Self {
        self.file_fields.extend(fields);
        self
    }

    /// Adds partition fields to the `TableSchema`.
    pub fn add_partition_fields(mut self, fields: Vec<Field>) -> Self {
        self.partition_fields.extend(fields);
        self
    }

    /// Builds the `TableSchema`, taking a `SchemaRef` as an argument, and returning a Result.
    pub fn build(self) -> TableSchema {
        // Combine file_fields and partition_fields as needed to create a file_projection
        // This is a simplistic approach, you might have more complex logic to combine these fields
        let mut fields = self.file_fields.clone();
        fields.extend(self.partition_fields);

        let schema = Schema::new(fields);

        let projection: Vec<usize> = (0..self.file_fields.len()).collect();

        TableSchema::new(Arc::new(schema.clone()), projection.clone())
    }
}

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
        let file_schema = &self.schema.project(&self.file_projection).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!(
                "Error projecting schema: {:?}",
                e
            ))
        })?;
        Ok(Arc::new(file_schema.clone()))
    }

    /// Get the fields for the table
    pub fn fields(&self) -> Fields {
        self.schema.fields().clone()
    }

    /// Get the schema for the table
    pub fn table_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
