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

use std::{collections::HashMap, sync::Arc};

use arrow::datatypes::{Field, Schema};
use noodles::sam::record::{
    data::field::{value::Array, Tag, Value},
    Data,
};

#[derive(Debug, Default)]
pub struct TagSchemaBuilder {
    /// The set of fields that have been seen.
    fields: HashMap<Tag, Field>,
}

impl TagSchemaBuilder {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn build(self) -> Schema {
        let mut values = self.fields.into_values().collect::<Vec<_>>();

        values.sort();

        Schema::new(values)
    }

    pub fn add_tag_data(&mut self, data: &Data) -> Result<(), Box<dyn std::error::Error>> {
        for (tag, value) in data.iter() {
            match value {
                Value::Character(_) | Value::String(_) | Value::Hex(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Utf8, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Utf8 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Utf8
                        )
                        .into());
                    }
                }
                Value::Int8(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Int8, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int8 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Int8
                        )
                        .into());
                    }
                }
                Value::Int16(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Int16, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int16 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Int16
                        )
                        .into());
                    }
                }
                Value::Int32(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Int32, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int32 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Int32
                        )
                        .into());
                    }
                }
                Value::UInt8(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::UInt8, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt8 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt8
                        )
                        .into());
                    }
                }
                Value::UInt16(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::UInt16, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt16 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt16
                        )
                        .into());
                    }
                }
                Value::UInt32(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::UInt32, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt32 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt32
                        )
                        .into());
                    }
                }
                Value::Float(_) => {
                    let field = self.fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Float32, false)
                    });

                    if field.data_type() != &arrow::datatypes::DataType::Float32 {
                        return Err(format!(
                            "tag {} has conflicting types: {:?} and {:?}",
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Float32
                        )
                        .into());
                    }
                }
                Value::Array(array) => match array {
                    Array::Int32(_) => {
                        let field = self.fields.entry(tag).or_insert_with(|| {
                            Field::new(
                                tag.to_string(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int32,
                                    true,
                                ))),
                                false,
                            )
                        });
                        if field.data_type()
                            != &arrow::datatypes::DataType::List(Arc::new(Field::new(
                                "item",
                                arrow::datatypes::DataType::Int32,
                                true,
                            )))
                        {
                            return Err(format!(
                                "tag {} has conflicting types: {:?} and {:?}",
                                tag,
                                field.data_type(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int32,
                                    true,
                                ))),
                            )
                            .into());
                        }
                    }
                    Array::Int16(_) => {
                        let field = self.fields.entry(tag).or_insert_with(|| {
                            Field::new(
                                tag.to_string(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int16,
                                    true,
                                ))),
                                false,
                            )
                        });

                        if field.data_type()
                            != &arrow::datatypes::DataType::List(Arc::new(Field::new(
                                "item",
                                arrow::datatypes::DataType::Int16,
                                true,
                            )))
                        {
                            return Err(format!(
                                "tag {} has conflicting types: {:?} and {:?}",
                                tag,
                                field.data_type(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int16,
                                    true,
                                ))),
                            )
                            .into());
                        }
                    }
                    Array::Int8(_) => {
                        let field = self.fields.entry(tag).or_insert_with(|| {
                            Field::new(
                                tag.to_string(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int8,
                                    true,
                                ))),
                                false,
                            )
                        });
                        if field.data_type()
                            != &arrow::datatypes::DataType::List(Arc::new(Field::new(
                                "item",
                                arrow::datatypes::DataType::Int8,
                                true,
                            )))
                        {
                            return Err(format!(
                                "tag {} has conflicting types: {:?} and {:?}",
                                tag,
                                field.data_type(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int8,
                                    true,
                                ))),
                            )
                            .into());
                        }
                    }
                    Array::Float(_) => {
                        let field = self.fields.entry(tag).or_insert_with(|| {
                            Field::new(
                                tag.to_string(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Float32,
                                    true,
                                ))),
                                false,
                            )
                        });

                        if field.data_type()
                            != &arrow::datatypes::DataType::List(Arc::new(Field::new(
                                "item",
                                arrow::datatypes::DataType::Float32,
                                true,
                            )))
                        {
                            return Err(format!(
                                "tag {} has conflicting types: {:?} and {:?}",
                                tag,
                                field.data_type(),
                                arrow::datatypes::DataType::List(Arc::new(Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Float32,
                                    true,
                                ))),
                            )
                            .into());
                        }
                    }
                    _ => {
                        return Err(
                            format!("tag {} has unsupported array type: {:?}", tag, array).into(),
                        )
                    }
                },
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use noodles::sam::record::data::field::tag::ALIGNMENT_HIT_COUNT;

    use crate::TagSchemaBuilder;

    #[test]
    pub fn test_tag_schema_builder() {
        let mut tag_schema_builder = TagSchemaBuilder::default();

        let mut data = noodles::sam::record::Data::default();
        data.insert(
            "RG".parse().unwrap(),
            noodles::sam::record::data::field::Value::String(String::from("foo")),
        );
        data.insert(
            ALIGNMENT_HIT_COUNT,
            noodles::sam::record::data::field::Value::Int32(5),
        );

        tag_schema_builder.add_tag_data(&data).unwrap();

        let schema = tag_schema_builder.build();

        assert_eq!(schema.fields().len(), 2);

        assert_eq!(schema.field(0).name(), "NH");
        assert_eq!(
            schema.field(0).data_type(),
            &arrow::datatypes::DataType::Int32
        );

        assert_eq!(schema.field(1).name(), "RG");
        assert_eq!(
            schema.field(1).data_type(),
            &arrow::datatypes::DataType::Utf8
        );
    }
}
