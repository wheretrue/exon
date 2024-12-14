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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::{ArrowError, Result};
use exon_common::TableSchema;
use noodles::sam::alignment::record_buf::data::field::{value::Array, Value};
use noodles::sam::alignment::record_buf::Data;

macro_rules! arrow_error {
    ($tag:expr, $field_type:expr, $expected_type:expr) => {
        Err(arrow::error::ArrowError::InvalidArgumentError(
            format!(
                "tag {} has conflicting types: {:?} and {:?}",
                $tag, $field_type, $expected_type,
            )
            .into(),
        ))
    };
}

/// Builds a schema for the BAM file.
pub struct SAMSchemaBuilder {
    file_fields: Vec<Field>,
    partition_fields: Vec<Field>,
    tags_data_type: Option<DataType>,
}

impl SAMSchemaBuilder {
    /// Creates a new SAM schema builder.
    pub fn new(file_fields: Vec<Field>, partition_fields: Vec<Field>) -> Self {
        Self {
            file_fields,
            partition_fields,
            tags_data_type: None,
        }
    }

    /// Set the partition fields.
    pub fn with_partition_fields(self, partition_fields: Vec<Field>) -> Self {
        Self {
            partition_fields,
            ..self
        }
    }

    /// Sets the data type for the tags field.
    pub fn with_tags_data_type(self, tags_data_type: DataType) -> Self {
        Self {
            tags_data_type: Some(tags_data_type),
            ..self
        }
    }

    /// Sets the data type for the tags field from the data.
    pub fn with_tags_data_type_from_data(self, data: &Data) -> Result<Self> {
        let mut fields = HashMap::new();

        for (tag, value) in data.iter() {
            let tag_name = std::str::from_utf8(tag.as_ref())?;

            match value {
                Value::Character(_) | Value::String(_) | Value::Hex(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::Utf8, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Utf8 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::Utf8
                        );
                    }
                }
                Value::Int8(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::Int8, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int8 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::Int8
                        );
                    }
                }
                Value::Int16(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::Int16, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int16 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::Int16
                        );
                    }
                }
                Value::Int32(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::Int32, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int32 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::Int32
                        );
                    }
                }
                Value::UInt8(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::UInt8, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt8 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt8
                        );
                    }
                }
                Value::UInt16(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::UInt16, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt16 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt16
                        );
                    }
                }
                Value::UInt32(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::UInt32, true)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt32 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt16
                        );
                    }
                }
                Value::Float(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag_name, arrow::datatypes::DataType::Float32, true)
                    });

                    if field.data_type() != &arrow::datatypes::DataType::Float32 {
                        return arrow_error!(
                            tag_name,
                            field.data_type(),
                            arrow::datatypes::DataType::Float32
                        );
                    }
                }
                Value::Array(array) => {
                    match array {
                        Array::Int32(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::Int32,
                                        true,
                                    ))),
                                    false,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Int32, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                        Array::Int16(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::Int16,
                                        true,
                                    ))),
                                    true,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Int16, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                        Array::Int8(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::Int8,
                                        true,
                                    ))),
                                    true,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Int8, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                        Array::UInt8(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::UInt8,
                                        true,
                                    ))),
                                    true,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::UInt8, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                        Array::UInt16(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::UInt16,
                                        true,
                                    ))),
                                    true,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::UInt16, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                        Array::UInt32(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::UInt32,
                                        true,
                                    ))),
                                    true,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::UInt32, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                        Array::Float(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
                                Field::new(
                                    tag_name,
                                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                                        "item",
                                        arrow::datatypes::DataType::Float32,
                                        true,
                                    ))),
                                    true,
                                )
                            });

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Float32, true),
                            ));

                            if field.data_type() != &expected_type {
                                return arrow_error!(tag_name, field.data_type(), expected_type);
                            }
                        }
                    }
                }
            }
        }

        // Make sure there are fields, otherwise return an error
        if fields.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "No fields found in the data".into(),
            ));
        }

        let data_type = DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(tag, field)| {
                    let data_type = field.data_type().clone();

                    // TODO: remove unwrap
                    let tag_name = std::str::from_utf8(tag.as_ref()).unwrap();

                    Field::new(tag_name, data_type, true)
                })
                .collect::<Vec<_>>(),
        ));

        Ok(self.with_tags_data_type(data_type))
    }

    /// Builds a schema for the BAM file.
    pub fn build(self) -> TableSchema {
        let mut fields = self.file_fields;

        if let Some(tags_data_type) = self.tags_data_type.clone() {
            let tags_field = Field::new("tags", tags_data_type, true);
            fields.push(tags_field);
        }

        let file_projection = (0..fields.len()).collect::<Vec<_>>();

        fields.extend_from_slice(&self.partition_fields);

        let file_schema = Schema::new(fields);

        TableSchema::new(Arc::new(file_schema), file_projection)
    }
}

impl Default for SAMSchemaBuilder {
    fn default() -> Self {
        let tags_data_type = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("tag", DataType::Utf8, false),
                Field::new("value", DataType::Utf8, true),
            ])),
            true,
        )));

        let quality_score_list =
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));

        Self::new(
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("flag", DataType::Int32, false),
                Field::new("reference", DataType::Utf8, true),
                Field::new("start", DataType::Int64, true),
                Field::new("end", DataType::Int64, true),
                Field::new("mapping_quality", DataType::Utf8, true),
                Field::new("cigar", DataType::Utf8, false),
                Field::new("mate_reference", DataType::Utf8, true),
                Field::new("sequence", DataType::Utf8, false),
                Field::new("quality_score", quality_score_list, false),
            ],
            vec![],
        )
        .with_tags_data_type(tags_data_type)
    }
}

#[cfg(test)]
mod tests {
    use noodles::sam::alignment::record::data::field::Tag;

    use super::*;

    #[test]
    fn test_build() -> Result<()> {
        let schema = SAMSchemaBuilder::default().build();

        assert_eq!(schema.fields().len(), 11);

        Ok(())
    }

    #[test]
    fn test_build_from_empty_data_errors() -> Result<()> {
        let data = Data::default();

        let schema = SAMSchemaBuilder::default().with_tags_data_type_from_data(&data);
        assert!(schema.is_err());

        Ok(())
    }

    #[test]
    fn test_parsing_data() -> Result<()> {
        let mut data = Data::default();

        data.insert(Tag::ALIGNMENT_HIT_COUNT, Value::from(1));
        data.insert(Tag::CELL_BARCODE_ID, Value::from("AA"));
        data.insert(Tag::ORIGINAL_UMI_QUALITY_SCORES, Value::from(vec![1, 2, 3]));

        let schema = SAMSchemaBuilder::default().with_tags_data_type_from_data(&data)?;

        let expected_fields = vec![
            Field::new(
                "BZ",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
            Field::new("CB", DataType::Utf8, false),
            Field::new("NH", DataType::UInt8, false),
        ];

        let tags_type = schema
            .tags_data_type
            .ok_or(ArrowError::InvalidArgumentError(
                "tags_data_type is None".into(),
            ))?;

        match tags_type {
            DataType::Struct(fields) => {
                let mut fields = fields.iter().collect::<Vec<_>>();
                fields.sort_by(|a, b| a.name().cmp(b.name()));

                assert_eq!(fields.len(), 3);

                fields
                    .iter()
                    .zip(expected_fields.iter())
                    .for_each(|(actual, expected)| {
                        assert_eq!(actual.name(), expected.name());
                        assert_eq!(actual.data_type(), expected.data_type());
                    });
            }
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "tags_data_type is not a struct".into(),
                ))
            }
        };

        Ok(())
    }
}
