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

use arrow::{
    array::{ArrayRef, GenericListBuilder, GenericStringBuilder, Int32Builder, StructBuilder},
    datatypes::{DataType, Field, Fields, Schema},
    error::ArrowError,
};
use datafusion::error::Result;
use noodles::sam::record::{
    data::field::{value::Array, Value},
    Data,
};
use noodles::sam::{alignment::Record, Header};

macro_rules! datafusion_error {
    ($tag:expr, $field_type:expr, $expected_type:expr) => {
        Err(datafusion::error::DataFusionError::Execution(
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
            match value {
                Value::Character(_) | Value::String(_) | Value::Hex(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Utf8, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Utf8 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Utf8
                        );
                    }
                }
                Value::Int8(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Int8, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int8 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Int8
                        );
                    }
                }
                Value::Int16(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Int16, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int16 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Int16
                        );
                    }
                }
                Value::Int32(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Int32, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::Int32 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::Int32
                        );
                    }
                }
                Value::UInt8(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::UInt8, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt8 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt8
                        );
                    }
                }
                Value::UInt16(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::UInt16, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt16 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt16
                        );
                    }
                }
                Value::UInt32(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::UInt32, false)
                    });
                    if field.data_type() != &arrow::datatypes::DataType::UInt32 {
                        return datafusion_error!(
                            tag,
                            field.data_type(),
                            arrow::datatypes::DataType::UInt16
                        );
                    }
                }
                Value::Float(_) => {
                    let field = fields.entry(tag).or_insert_with(|| {
                        Field::new(tag.to_string(), arrow::datatypes::DataType::Float32, false)
                    });

                    if field.data_type() != &arrow::datatypes::DataType::Float32 {
                        return datafusion_error!(
                            tag,
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
                                    tag.to_string(),
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
                                return datafusion_error!(tag, field.data_type(), expected_type);
                            }
                        }
                        Array::Int16(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
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

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Int16, true),
                            ));

                            if field.data_type() != &expected_type {
                                return datafusion_error!(tag, field.data_type(), expected_type);
                            }
                        }
                        Array::Int8(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
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

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Int8, true),
                            ));

                            if field.data_type() != &expected_type {
                                return datafusion_error!(tag, field.data_type(), expected_type);
                            }
                        }
                        Array::Float(_) => {
                            let field = fields.entry(tag).or_insert_with(|| {
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

                            let expected_type = arrow::datatypes::DataType::List(Arc::new(
                                Field::new("item", arrow::datatypes::DataType::Float32, true),
                            ));

                            if field.data_type() != &expected_type {
                                return datafusion_error!(tag, field.data_type(), expected_type);
                            }
                        }
                        _ => {
                            return Err(datafusion::error::DataFusionError::Execution(format!(
                                "tag {} has unsupported array type",
                                tag
                            )))
                        }
                    }
                }
            }
        }

        let data_type = DataType::Struct(Fields::from(
            fields
                .into_iter()
                .map(|(tag, field)| {
                    let data_type = field.data_type().clone();
                    Field::new(tag.to_string(), data_type, false)
                })
                .collect::<Vec<_>>(),
        ));

        Ok(self.with_tags_data_type(data_type))
    }

    /// Builds a schema for the BAM file.
    pub fn build(self) -> (Schema, Vec<usize>) {
        let mut fields = self.file_fields;

        if let Some(tags_data_type) = self.tags_data_type.clone() {
            let tags_field = Field::new("tags", tags_data_type, true);
            fields.push(tags_field);
        }

        let file_projection = (0..fields.len()).collect::<Vec<_>>();

        fields.extend_from_slice(&self.partition_fields);
        (Schema::new(fields), file_projection)
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

        Self::new(
            vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("flag", DataType::Int32, false),
                Field::new("reference", DataType::Utf8, true),
                Field::new("start", DataType::Int32, true),
                Field::new("end", DataType::Int32, true),
                Field::new("mapping_quality", DataType::Utf8, true),
                Field::new("cigar", DataType::Utf8, false),
                Field::new("mate_reference", DataType::Utf8, true),
                Field::new("sequence", DataType::Utf8, false),
                Field::new("quality_score", DataType::Utf8, false),
            ],
            vec![],
        )
        .with_tags_data_type(tags_data_type)
    }
}

/// Builds an vector of arrays from a SAM file.
pub struct SAMArrayBuilder {
    names: GenericStringBuilder<i32>,
    flags: Int32Builder,
    references: GenericStringBuilder<i32>,
    starts: Int32Builder,
    ends: Int32Builder,
    mapping_qualities: GenericStringBuilder<i32>,
    cigar: GenericStringBuilder<i32>,
    mate_references: GenericStringBuilder<i32>,
    sequences: GenericStringBuilder<i32>,
    quality_scores: GenericStringBuilder<i32>,

    tags: GenericListBuilder<i32, StructBuilder>,

    projection: Vec<usize>,

    rows: usize,

    header: Header,
}

impl SAMArrayBuilder {
    /// Creates a new SAM array builder.
    pub fn create(header: Header, projection: Vec<usize>) -> Self {
        let tag_field = Field::new("tag", DataType::Utf8, false);
        let value_field = Field::new("value", DataType::Utf8, true);

        let tag = StructBuilder::new(
            Fields::from(vec![tag_field, value_field]),
            vec![
                Box::new(GenericStringBuilder::<i32>::new()),
                Box::new(GenericStringBuilder::<i32>::new()),
            ],
        );

        Self {
            names: GenericStringBuilder::<i32>::new(),
            flags: Int32Builder::new(),
            references: GenericStringBuilder::<i32>::new(),
            starts: Int32Builder::new(),
            ends: Int32Builder::new(),
            mapping_qualities: GenericStringBuilder::<i32>::new(),
            cigar: GenericStringBuilder::<i32>::new(),
            mate_references: GenericStringBuilder::<i32>::new(),
            sequences: GenericStringBuilder::<i32>::new(),
            quality_scores: GenericStringBuilder::<i32>::new(),

            tags: GenericListBuilder::new(tag),

            projection,

            rows: 0,

            header,
        }
    }

    /// Returns the number of records in the builder.
    pub fn len(&self) -> usize {
        self.rows
    }

    /// Returns whether the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a record to the builder.
    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => self.names.append_option(record.read_name()),
                1 => {
                    let flag_bits = record.flags().bits();
                    self.flags.append_value(flag_bits as i32);
                }
                2 => {
                    let reference_name = match record.reference_sequence(&self.header) {
                        Some(Ok((name, _))) => Some(name.as_str()),
                        Some(Err(_)) => None,
                        None => None,
                    };
                    self.references.append_option(reference_name);
                }
                3 => {
                    self.starts
                        .append_option(record.alignment_start().map(|v| v.get() as i32));
                }
                4 => {
                    self.ends
                        .append_option(record.alignment_end().map(|v| v.get() as i32));
                }
                5 => {
                    self.mapping_qualities
                        .append_option(record.mapping_quality().map(|v| v.get().to_string()));
                }
                6 => {
                    let cigar_string = record.cigar().to_string();
                    self.cigar.append_value(cigar_string.as_str());
                }
                7 => {
                    let mate_reference_name = match record.mate_reference_sequence(&self.header) {
                        Some(Ok((name, _))) => Some(name.as_str()),
                        Some(Err(_)) => None,
                        None => None,
                    };
                    self.mate_references.append_option(mate_reference_name);
                }
                8 => {
                    let sequence_string = record.sequence().to_string();
                    self.sequences.append_value(sequence_string.as_str());
                }
                9 => {
                    let quality_scores = record.quality_scores();
                    self.quality_scores
                        .append_value(quality_scores.to_string().as_str());
                }
                10 => {
                    let data = record.data();
                    let tags = data.keys();

                    let tag_struct = self.tags.values();
                    for tag in tags {
                        let tag_value = data.get(&tag).unwrap();

                        let tag_value_string = tag_value.to_string();

                        tag_struct
                            .field_builder::<GenericStringBuilder<i32>>(0)
                            .unwrap()
                            .append_value(tag.to_string());

                        tag_struct
                            .field_builder::<GenericStringBuilder<i32>>(1)
                            .unwrap()
                            .append_value(tag_value_string);

                        tag_struct.append(true);
                    }
                    self.tags.append(true);
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Invalid column index {} for SAM",
                        col_idx
                    )))
                }
            }
        }

        self.rows += 1;

        Ok(())
    }

    /// Finishes the builder and returns an vector of arrays.
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => arrays.push(Arc::new(self.names.finish())),
                1 => arrays.push(Arc::new(self.flags.finish())),
                2 => arrays.push(Arc::new(self.references.finish())),
                3 => arrays.push(Arc::new(self.starts.finish())),
                4 => arrays.push(Arc::new(self.ends.finish())),
                5 => arrays.push(Arc::new(self.mapping_qualities.finish())),
                6 => arrays.push(Arc::new(self.cigar.finish())),
                7 => arrays.push(Arc::new(self.mate_references.finish())),
                8 => arrays.push(Arc::new(self.sequences.finish())),
                9 => arrays.push(Arc::new(self.quality_scores.finish())),
                10 => arrays.push(Arc::new(self.tags.finish())),
                _ => panic!("Invalid column index {} for SAM", col_idx),
            }
        }

        arrays
    }
}
