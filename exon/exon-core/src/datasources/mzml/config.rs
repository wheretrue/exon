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

use arrow::datatypes::{DataType, Field, Fields, Schema};
use object_store::ObjectStore;

use exon_common::{TableSchema, DEFAULT_BATCH_SIZE};

pub struct MzMLSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
}

impl MzMLSchemaBuilder {
    pub fn add_partition_fields(&mut self, partition_fields: Vec<Field>) {
        self.partition_fields.extend(partition_fields);
    }

    pub fn build(self) -> TableSchema {
        let mut fields = self.file_fields.clone();
        fields.extend(self.partition_fields);

        let schema = Schema::new(fields);

        let projection: Vec<usize> = (0..self.file_fields.len()).collect();

        TableSchema::new(Arc::new(schema.clone()), projection.clone())
    }
}

impl Default for MzMLSchemaBuilder {
    fn default() -> Self {
        Self {
            file_fields: file_fields(),
            partition_fields: vec![],
        }
    }
}

/// Configuration for a MzML data source.
pub struct MzMLConfig {
    /// The number of rows to read at a time.
    pub batch_size: usize,

    /// The schema to use for MzML files.
    pub file_schema: Arc<Schema>,

    /// The object store to use for reading MzML files.
    pub object_store: Arc<dyn ObjectStore>,

    /// Any projections to apply to the resulting batches.
    pub projection: Option<Vec<usize>>,
}

impl MzMLConfig {
    /// Create a new MzML configuration.
    pub fn new(object_store: Arc<dyn ObjectStore>, file_schema: Arc<Schema>) -> Self {
        Self {
            object_store,
            batch_size: DEFAULT_BATCH_SIZE,
            file_schema,
            projection: None,
        }
    }

    /// Set the batch size.
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

fn file_fields() -> Vec<Field> {
    let mz_fields = Fields::from(vec![Field::new(
        "mz",
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        true,
    )]);

    let mz_field = Field::new("mz", DataType::Struct(mz_fields), true);

    let intensity_fields = Fields::from(vec![Field::new(
        "intensity",
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        true,
    )]);

    let intensity_field = Field::new("intensity", DataType::Struct(intensity_fields), true);

    let wavelength_fields = Fields::from(vec![Field::new(
        "wavelength",
        DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        true,
    )]);

    let wavelength_field = Field::new("wavelength", DataType::Struct(wavelength_fields), true);

    // An individual cvParam
    let cv_param_struct = Field::new(
        "values",
        DataType::Struct(Fields::from(vec![
            Field::new("accession", DataType::Utf8, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Utf8, true),
        ])),
        true,
    );

    let cv_params_field = Field::new_map(
        "cv_params",
        "entries",
        Field::new("keys", DataType::Utf8, false),
        Field::new(
            "values",
            DataType::Struct(Fields::from(vec![
                Field::new("accession", DataType::Utf8, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        ),
        false,
        true,
    );

    let cv_key_field = Field::new("keys", DataType::Utf8, false);

    // A map of cvParams to their values (DataType::Utf8 to cvParamStruct)
    let isolation_window = Field::new_map(
        "isolation_window",
        "entries",
        cv_key_field.clone(),
        cv_param_struct.clone(),
        false,
        true,
    );

    let activation = Field::new_map(
        "activation",
        "entries",
        cv_key_field,
        cv_param_struct,
        false,
        true,
    );

    // A precursor is a struct
    let precursor = Field::new(
        "item",
        DataType::Struct(Fields::from(vec![isolation_window, activation])),
        true,
    );

    // A precursor list is a list of precursors
    let precursor_list = Field::new("precursor_list", DataType::List(Arc::new(precursor)), true);

    vec![
        Field::new("id", DataType::Utf8, false),
        mz_field,
        intensity_field,
        wavelength_field,
        cv_params_field,
        precursor_list,
    ]
}
