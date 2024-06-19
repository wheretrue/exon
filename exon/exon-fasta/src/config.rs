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

use std::{str::FromStr, sync::Arc};

use arrow::datatypes::{DataType, Field, SchemaRef};
use exon_common::TableSchema;
use noodles::core::Region;
use object_store::ObjectStore;

use crate::ExonFASTAError;

#[derive(Debug, Clone)]
pub enum SequenceDataType {
    Utf8,
    LargeUtf8,
    IntegerEncodeProtein,
    IntegerEncodeDNA,
}

impl FromStr for SequenceDataType {
    type Err = ExonFASTAError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "utf8" => Ok(Self::Utf8),
            "large_utf8" => Ok(Self::LargeUtf8),
            "integer_encode_protein" => Ok(Self::IntegerEncodeProtein),
            "integer_encode_dna" => Ok(Self::IntegerEncodeDNA),
            _ => Err(ExonFASTAError::InvalidSequenceDataType(s.to_string())),
        }
    }
}

/// Configuration for a FASTA data source.
#[derive(Debug)]
pub struct FASTAConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema of the FASTA file.
    pub file_schema: SchemaRef,

    /// The object store to use for reading FASTA files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,

    /// How many bytes to pre-allocate for the sequence.
    pub fasta_sequence_buffer_capacity: usize,

    /// The type of data to use for the sequence.
    pub sequence_data_type: SequenceDataType,

    /// An optional region to read from.
    pub region: Option<Region>,

    /// An optional region file to read from.
    pub region_file: Option<String>,
}

impl FASTAConfig {
    /// Create a new FASTA configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: SchemaRef) -> Self {
        Self {
            object_store,
            file_schema,
            batch_size: exon_common::DEFAULT_BATCH_SIZE,
            projection: None,
            fasta_sequence_buffer_capacity: 384,
            sequence_data_type: SequenceDataType::Utf8,
            region: None,
            region_file: None,
        }
    }

    /// Create a new FASTA configuration with a given region.
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }

    /// Create a new FASTA configuration with a given region file.
    pub fn with_region_file(mut self, region_file: String) -> Self {
        self.region_file = Some(region_file);
        self
    }

    /// Create a new FASTA configuration with a given batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Get the projection, returning the identity projection if none is set.
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

    /// Create a new FASTA configuration with a given projection.
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

    /// Create a new FASTA configuration with a given sequence capacity.
    pub fn with_fasta_sequence_buffer_capacity(
        mut self,
        fasta_sequence_buffer_capacity: usize,
    ) -> Self {
        self.fasta_sequence_buffer_capacity = fasta_sequence_buffer_capacity;
        self
    }

    pub fn with_sequence_data_type(mut self, sequence_data_type: SequenceDataType) -> Self {
        self.sequence_data_type = sequence_data_type;
        self
    }
}

pub struct FASTASchemaBuilder {
    /// The fields of the schema.
    fields: Vec<Field>,

    /// The partition fields to potentially add to the schema.
    partition_fields: Vec<Field>,

    /// The sequence data type.
    sequence_data_type: SequenceDataType,
}

impl Default for FASTASchemaBuilder {
    fn default() -> Self {
        Self {
            fields: vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, true),
                Field::new("sequence", DataType::Utf8, false),
            ],
            partition_fields: vec![],
            sequence_data_type: SequenceDataType::Utf8,
        }
    }
}

impl FASTASchemaBuilder {
    /// Set the type of sequence to store.
    pub fn with_sequence_data_type(mut self, sequence_data_type: SequenceDataType) -> Self {
        self.sequence_data_type = sequence_data_type;
        self
    }

    /// Extend the partition fields with the given fields.
    pub fn with_partition_fields(mut self, partition_fields: Vec<Field>) -> Self {
        self.partition_fields.extend(partition_fields);
        self
    }

    pub fn build(&mut self) -> TableSchema {
        let mut fields = self.fields.clone();

        match self.sequence_data_type {
            SequenceDataType::Utf8 => {
                let field = Field::new("sequence", DataType::Utf8, true);
                fields[2] = field;
            }
            SequenceDataType::LargeUtf8 => {
                let field = Field::new("sequence", DataType::LargeUtf8, true);
                fields[2] = field;
            }
            SequenceDataType::IntegerEncodeProtein => {
                let data_type = DataType::List(Arc::new(Field::new("item", DataType::Int8, true)));

                let field = Field::new("sequence", data_type, true);
                fields[2] = field;
            }
            SequenceDataType::IntegerEncodeDNA => {
                let data_type = DataType::List(Arc::new(Field::new("item", DataType::Int8, true)));

                let field = Field::new("sequence", data_type, true);
                fields[2] = field;
            }
        }

        let file_field_projection = self
            .fields
            .iter()
            .enumerate()
            .map(|(i, _)| i)
            .collect::<Vec<_>>();

        fields.extend(self.partition_fields.clone());

        let arrow_schema = Arc::new(arrow::datatypes::Schema::new(fields.clone()));
        TableSchema::new(arrow_schema, file_field_projection)
    }
}
