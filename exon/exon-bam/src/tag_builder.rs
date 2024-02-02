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

use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, GenericListBuilder, GenericStringBuilder,
        StructBuilder, UInt8Builder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use noodles::sam::{
    alignment::record_buf::{data::field::Value, Data},
    header::record::value::map::header::Tag,
};

pub enum TagsBuilder {
    Map(TagsMapBuilder),
    Struct(TagsStructBuilder),
}

impl TryFrom<&DataType> for TagsBuilder {
    type Error = ArrowError;

    fn try_from(data_type: &DataType) -> Result<Self, Self::Error> {
        match data_type {
            DataType::List(_) => Ok(TagsBuilder::Map(TagsMapBuilder::new())),
            DataType::Struct(fields) => {
                Ok(TagsBuilder::Struct(TagsStructBuilder::try_from(fields)?))
            }
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "Invalid data type {:?} for tags",
                data_type
            ))),
        }
    }
}

impl Default for TagsBuilder {
    fn default() -> Self {
        TagsBuilder::Map(TagsMapBuilder::new())
    }
}

impl TagsBuilder {
    pub fn append(&mut self, data: &Data) -> Result<(), ArrowError> {
        match self {
            TagsBuilder::Map(builder) => builder.append(data),
            TagsBuilder::Struct(builder) => builder.append(data),
        }
    }

    pub fn finish(&mut self) -> ArrayRef {
        match self {
            TagsBuilder::Map(builder) => Arc::new(builder.finish()),
            TagsBuilder::Struct(builder) => Arc::new(builder.finish()),
        }
    }
}

pub struct TagsStructBuilder {
    builder: StructBuilder,
    fields: Fields,
}

impl TryFrom<&Fields> for TagsStructBuilder {
    type Error = ArrowError;

    fn try_from(fields: &Fields) -> Result<Self, Self::Error> {
        let mut builders = Vec::<Box<dyn ArrayBuilder>>::new();

        let capacity = 50;

        for field in fields.clone().iter() {
            let builder = make_builder(field.data_type(), capacity);
            builders.push(builder);
        }

        let builder = StructBuilder::new(fields.clone(), builders);

        Ok(Self {
            builder,
            fields: fields.clone(),
        })
    }
}

impl TagsStructBuilder {
    pub fn finish(&mut self) -> arrow::array::StructArray {
        self.builder.finish()
    }

    pub fn append(&mut self, data: &Data) -> Result<(), ArrowError> {
        for (i, f) in self.fields.iter().enumerate() {
            let tag_name = f.name();

            let tag = Tag::from_str(tag_name).map_err(|e| {
                ArrowError::InvalidArgumentError(format!(
                    "Invalid tag name {:?} for tags: {}",
                    tag_name, e
                ))
            })?;

            let tag_option = data.get(tag.as_ref());

            match tag_option {
                None => match f.data_type() {
                    DataType::Utf8 => {
                        self.builder
                            .field_builder::<GenericStringBuilder<i32>>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::UInt8 => {
                        self.builder
                            .field_builder::<UInt8Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Invalid data type {:?} for tag {}",
                            f.data_type(),
                            tag_name
                        )))
                    }
                },
                Some(tag_value) => match f.data_type() {
                    DataType::Utf8 => match tag_value {
                        Value::Character(c) => {
                            let tag_value_str = c.to_string();
                            self.builder
                                .field_builder::<GenericStringBuilder<i32>>(i)
                                .unwrap()
                                .append_value(tag_value_str);
                        }
                        Value::String(s) => {
                            let tag_value_str = std::str::from_utf8(s.as_ref())?.to_string();
                            self.builder
                                .field_builder::<GenericStringBuilder<i32>>(i)
                                .unwrap()
                                .append_value(tag_value_str);
                        }
                        _ => {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {}",
                                tag_value, tag_name
                            )))
                        }
                    },
                    DataType::UInt8 => {
                        if let Value::UInt8(int) = tag_value {
                            let u8_value = *int;
                            self.builder
                                .field_builder::<UInt8Builder>(i)
                                .unwrap()
                                .append_value(u8_value);
                        } else {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {}",
                                tag_value, tag_name
                            )));
                        }
                    }
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Invalid data type {:?} for tag {}",
                            f.data_type(),
                            tag_name
                        )))
                    }
                },
            }
        }

        self.builder.append(true);
        Ok(())
    }
}

pub struct TagsMapBuilder {
    builder: GenericListBuilder<i32, StructBuilder>,
}

impl TagsMapBuilder {
    pub fn new() -> Self {
        let tag_field = Field::new("tag", DataType::Utf8, false);
        let value_field = Field::new("value", DataType::Utf8, true);

        let tag = StructBuilder::new(
            Fields::from(vec![tag_field, value_field]),
            vec![
                Box::new(GenericStringBuilder::<i32>::new()),
                Box::new(GenericStringBuilder::<i32>::new()),
            ],
        );

        let builder = GenericListBuilder::new(tag);

        Self { builder }
    }

    pub fn append(&mut self, data: &Data) -> Result<(), arrow::error::ArrowError> {
        let tags = data.keys();

        let tag_struct = self.builder.values();

        for tag in tags {
            let tag_str = std::str::from_utf8(tag.as_ref())?;
            // let tag_value = data.get(&tag);

            match data.get(&tag) {
                None => {
                    tag_struct
                        .field_builder::<GenericStringBuilder<i32>>(0)
                        .unwrap()
                        .append_value(tag_str);

                    tag_struct
                        .field_builder::<GenericStringBuilder<i32>>(1)
                        .unwrap()
                        .append_null();
                }
                Some(tag_value) => {
                    if tag_value.is_int() {
                        let tag_value_str = tag_value.as_int().map(|v| v.to_string());

                        tag_struct
                            .field_builder::<GenericStringBuilder<i32>>(0)
                            .unwrap()
                            .append_value(tag_str);

                        tag_struct
                            .field_builder::<GenericStringBuilder<i32>>(1)
                            .unwrap()
                            .append_option(tag_value_str);
                    } else {
                        match tag_value {
                            Value::String(tag_value_str) => {
                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(0)
                                    .unwrap()
                                    .append_value(tag_str);

                                let tag_value_str =
                                    std::str::from_utf8(tag_value_str.as_ref())?.to_string();
                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(1)
                                    .unwrap()
                                    .append_value(tag_value_str);
                            }
                            Value::Character(tag_value_char) => {
                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(0)
                                    .unwrap()
                                    .append_value(tag_str);

                                let tag_value_char = *tag_value_char as char;
                                let tag_value_str = tag_value_char.to_string();

                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(1)
                                    .unwrap()
                                    .append_value(tag_value_str);
                            }
                            Value::Float(f) => {
                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(0)
                                    .unwrap()
                                    .append_value(tag_str);

                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(1)
                                    .unwrap()
                                    .append_value(f.to_string());
                            }
                            _ => {
                                return Err(ArrowError::InvalidArgumentError(format!(
                                    "Invalid tag value {:?} for tag {}",
                                    tag_value, tag_str
                                )))
                            }
                        }
                    }
                }
            }

            tag_struct.append(true);
        }
        self.builder.append(true);

        Ok(())
    }

    pub fn finish(&mut self) -> arrow::array::ListArray {
        self.builder.finish()
    }
}
