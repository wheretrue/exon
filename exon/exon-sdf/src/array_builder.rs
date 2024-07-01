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

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{GenericStringBuilder, StructBuilder},
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};

use crate::{record::Data, Record};

struct DataArrayBuilder {
    inner: StructBuilder,
    field_to_index: HashMap<String, usize>,
}

impl DataArrayBuilder {
    pub fn new(fields: Fields) -> Self {
        let field_to_index = fields
            .iter()
            .enumerate()
            .map(|(i, field)| (field.name().to_string(), i))
            .collect();

        let inner = StructBuilder::from_fields(fields, 100);
        DataArrayBuilder {
            inner,
            field_to_index,
        }
    }

    pub fn try_new(field: &Arc<Field>) -> Result<Self, arrow::error::ArrowError> {
        let fields = match field.data_type() {
            DataType::Struct(s) => s,
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "format field is not a struct".to_string(),
                ))
            }
        };

        Ok(DataArrayBuilder::new(fields.clone()))
    }

    pub fn append_value(&mut self, data: &Data) -> Result<(), arrow::error::ArrowError> {
        for datum in data {
            let field_idx = self.field_to_index.get(datum.header()).ok_or(
                arrow::error::ArrowError::InvalidArgumentError(format!(
                    "Field {} not found in schema",
                    datum.header()
                )),
            )?;

            let value = datum.data();

            self.inner
                .field_builder::<GenericStringBuilder<i32>>(*field_idx)
                .unwrap()
                .append_value(value);
        }

        self.inner.append(true);

        Ok(())
    }

    pub fn finish(&mut self) -> arrow::array::ArrayRef {
        Arc::new(self.inner.finish())
    }
}

// Structured Data File (SDF) Array Builder
pub(crate) struct SDFArrayBuilder {
    data: DataArrayBuilder,
    n_rows: usize,
}

impl SDFArrayBuilder {
    pub fn new(fields: Fields) -> Self {
        let (_, data_field) = fields.find("data").unwrap();

        let data = DataArrayBuilder::try_new(data_field).unwrap();
        SDFArrayBuilder { n_rows: 0, data }
    }

    pub fn append_value(&mut self, record: Record) {
        self.n_rows += 1;

        self.data.append_value(record.data()).unwrap();
    }

    pub fn len(&self) -> usize {
        self.n_rows
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> Vec<arrow::array::ArrayRef> {
        let finished_data = self.data.finish();

        vec![finished_data]
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema};

    use crate::Record;

    use super::SDFArrayBuilder;

    #[test]
    fn test_append_to_sdf_array_builder() {
        let mut record = Record::default();
        record
            .data_mut()
            .push("canonical_smiles".to_string(), "CCC".to_string());

        let data_fields = vec![Field::new(
            "canonical_smiles",
            arrow::datatypes::DataType::Utf8,
            true,
        )];
        let schema = Schema::new(vec![Field::new_struct("data", data_fields, true)]);

        let mut sdf_array_builder = SDFArrayBuilder::new(schema.fields().clone());

        sdf_array_builder.append_value(record);

        assert_eq!(sdf_array_builder.len(), 1);
    }
}
