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
    array::{GenericStringBuilder, StringBuilder, StructBuilder},
    datatypes::{DataType, Field, Fields},
};

use crate::{record::Data, ExonSDFError, Record};

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

    pub fn try_new(field: &Arc<Field>) -> crate::Result<Self> {
        let fields = match field.data_type() {
            DataType::Struct(s) => s,
            _ => {
                return Err(crate::ExonSDFError::InvalidInput(
                    "Data field must be a struct".to_string(),
                ))
            }
        };

        Ok(DataArrayBuilder::new(fields.clone()))
    }

    pub fn append_value(&mut self, data: &Data) -> crate::Result<()> {
        for datum in data {
            let re = regex::Regex::new(r"<(.*?)>").unwrap();

            let parsed = re.captures(datum.header()).ok_or(
                arrow::error::ArrowError::InvalidArgumentError(format!(
                    "Invalid header: {}",
                    datum.header()
                )),
            )?;
            let header = parsed.get(1).unwrap().as_str();

            let field_idx = self
                .field_to_index
                .get(header)
                .ok_or(ExonSDFError::MissingDataFieldInSchema(header.to_string()))?;

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
    header: StringBuilder,
    atom_count: arrow::array::UInt32Builder,
    bond_count: arrow::array::UInt32Builder,
    data: DataArrayBuilder,
    n_rows: usize,
}

impl SDFArrayBuilder {
    pub fn new(fields: Fields) -> crate::Result<Self> {
        let header = StringBuilder::new();
        let atom_count = arrow::array::UInt32Builder::new();
        let bond_count = arrow::array::UInt32Builder::new();

        let (_, data_field) = fields
            .find("data")
            .ok_or(crate::ExonSDFError::MissingDataField)?;

        let data = DataArrayBuilder::try_new(data_field)?;

        Ok(SDFArrayBuilder {
            n_rows: 0,
            data,
            header,
            atom_count,
            bond_count,
        })
    }

    pub fn append_value(&mut self, record: Record) -> crate::Result<()> {
        self.n_rows += 1;

        self.header.append_value(record.header());
        self.atom_count.append_value(record.atom_count() as u32);
        self.bond_count.append_value(record.bond_count() as u32);

        self.data.append_value(record.data())?;

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.n_rows
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn finish(&mut self) -> Vec<arrow::array::ArrayRef> {
        let finished_header = Arc::new(self.header.finish());
        let finished_atom_count = Arc::new(self.atom_count.finish());
        let finished_bond_count = Arc::new(self.bond_count.finish());
        let finished_data = self.data.finish();

        vec![
            finished_header,
            finished_atom_count,
            finished_bond_count,
            finished_data,
        ]
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema};

    use crate::Record;

    use super::SDFArrayBuilder;

    #[test]
    fn test_append_to_sdf_array_builder() -> Result<(), Box<dyn std::error::Error>> {
        let mut record = Record::default();
        record
            .data_mut()
            .push(">  <canonical_smiles>".to_string(), "CCC".to_string());

        let data_fields = vec![Field::new(
            "canonical_smiles",
            arrow::datatypes::DataType::Utf8,
            true,
        )];
        let schema = Schema::new(vec![Field::new_struct("data", data_fields, true)]);

        let mut sdf_array_builder = SDFArrayBuilder::new(schema.fields().clone())?;

        sdf_array_builder.append_value(record)?;

        assert_eq!(sdf_array_builder.len(), 1);

        Ok(())
    }
}
