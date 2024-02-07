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
        make_builder, ArrayBuilder, ArrayRef, Float32Builder, GenericListBuilder,
        GenericStringBuilder, Int16Builder, Int32Builder, Int8Builder, StructBuilder,
        UInt16Builder, UInt32Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use noodles::sam::{
    alignment::record_buf::{
        data::field::{value::Array, Value},
        Data,
    },
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
        let capacity = 50;
        let mut builders = Vec::<Box<dyn ArrayBuilder>>::new();

        for field in fields.clone().iter() {
            match field.data_type() {
                DataType::List(itype) => match itype.data_type() {
                    DataType::Int32 => {
                        let builder =
                            GenericListBuilder::<i32, Int32Builder>::new(Int32Builder::new());

                        builders.push(Box::new(builder));
                    }
                    DataType::Float32 => {
                        let builder =
                            GenericListBuilder::<i32, Float32Builder>::new(Float32Builder::new());

                        builders.push(Box::new(builder));
                    }
                    DataType::Utf8 => {
                        let builder = GenericListBuilder::<i32, GenericStringBuilder<i32>>::new(
                            GenericStringBuilder::<i32>::new(),
                        );

                        builders.push(Box::new(builder));
                    }
                    DataType::UInt8 => {
                        let builder =
                            GenericListBuilder::<i32, UInt8Builder>::new(UInt8Builder::new());
                        builders.push(Box::new(builder));
                    }
                    DataType::Int8 => {
                        let builder =
                            GenericListBuilder::<i32, Int8Builder>::new(Int8Builder::new());
                        builders.push(Box::new(builder));
                    }
                    DataType::Int16 => {
                        let builder =
                            GenericListBuilder::<i32, Int16Builder>::new(Int16Builder::new());
                        builders.push(Box::new(builder));
                    }
                    DataType::UInt16 => {
                        let builder =
                            GenericListBuilder::<i32, UInt16Builder>::new(UInt16Builder::new());
                        builders.push(Box::new(builder));
                    }
                    DataType::UInt32 => {
                        let builder =
                            GenericListBuilder::<i32, UInt32Builder>::new(UInt32Builder::new());
                        builders.push(Box::new(builder));
                    }
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Invalid data type {:?} for tag list",
                            itype.data_type()
                        )))
                    }
                },
                dt => {
                    let builder = make_builder(dt, capacity);
                    builders.push(builder);
                }
            }
        }

        let struct_builder = StructBuilder::new(fields.clone(), builders);

        Ok(Self {
            builder: struct_builder,
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
                    DataType::Int8 => {
                        self.builder
                            .field_builder::<Int8Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::Int16 => {
                        self.builder
                            .field_builder::<Int16Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::UInt16 => {
                        self.builder
                            .field_builder::<UInt16Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::UInt32 => {
                        self.builder
                            .field_builder::<UInt32Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::Int32 => {
                        self.builder
                            .field_builder::<Int32Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::Float32 => {
                        self.builder
                            .field_builder::<Float32Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Invalid null data type {:?} for tag {}",
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
                        Value::Hex(s) => {
                            let tag_value_str = std::str::from_utf8(s.as_ref())?.to_string();
                            self.builder
                                .field_builder::<GenericStringBuilder<i32>>(i)
                                .unwrap()
                                .append_value(tag_value_str);
                        }
                        _ => {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
                            )))
                        }
                    },
                    DataType::Int8 => {
                        if let Value::Int8(int) = tag_value {
                            let i8_value = *int;
                            self.builder
                                .field_builder::<Int8Builder>(i)
                                .unwrap()
                                .append_value(i8_value);
                        } else {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
                            )));
                        }
                    }
                    DataType::UInt8 => {
                        if let Value::UInt8(int) = tag_value {
                            let u8_value = *int;
                            self.builder
                                .field_builder::<UInt8Builder>(i)
                                .unwrap()
                                .append_value(u8_value);
                        } else {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
                            )));
                        }
                    }
                    DataType::Int16 => {
                        if let Value::Int16(int) = tag_value {
                            let i16_value = *int;
                            self.builder
                                .field_builder::<Int16Builder>(i)
                                .unwrap()
                                .append_value(i16_value);
                        } else {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
                            )));
                        }
                    }
                    DataType::Int32 => {
                        if let Value::Int32(int) = tag_value {
                            let i32_value = *int;
                            self.builder
                                .field_builder::<Int32Builder>(i)
                                .unwrap()
                                .append_value(i32_value);
                        } else {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
                            )));
                        }
                    }
                    DataType::List(field) => match (field.data_type(), tag_value) {
                        (DataType::UInt8, Value::Array(Array::UInt8(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, UInt8Builder>>(i)
                                .unwrap();

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (DataType::Int8, Value::Array(Array::Int8(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, Int8Builder>>(i)
                                .ok_or(ArrowError::InvalidArgumentError(format!(
                                    "Cannot extract builder for {} of field {}",
                                    tag_name, i
                                )))?;

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (DataType::UInt16, Value::Array(Array::UInt16(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, UInt16Builder>>(i)
                                .unwrap();

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (DataType::Int16, Value::Array(Array::Int16(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, Int16Builder>>(i)
                                .unwrap();

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (DataType::Float32, Value::Array(Array::Float(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                .ok_or(ArrowError::InvalidArgumentError(format!(
                                    "Cannot extract builder for {:?} for tag {} a {}",
                                    tag_value,
                                    tag_name,
                                    f.data_type()
                                )))?;

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (DataType::UInt32, Value::Array(Array::UInt32(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, UInt32Builder>>(i)
                                .ok_or(ArrowError::InvalidArgumentError(format!(
                                    "Cannot extract builder for {:?} for tag {} a {}",
                                    tag_value,
                                    tag_name,
                                    f.data_type()
                                )))?;

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (DataType::Int32, Value::Array(Array::Int32(arr))) => {
                            let builder = self
                                .builder
                                .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                                .ok_or(ArrowError::InvalidArgumentError(format!(
                                    "Cannot extract builder for {:?} for tag {} a {}",
                                    tag_value,
                                    tag_name,
                                    f.data_type()
                                )))?;

                            let builder_values = builder.values();

                            for v in arr.iter() {
                                builder_values.append_value(*v);
                            }

                            builder.append(true);
                        }
                        (_, _) => {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
                            )))
                        }
                    },
                    DataType::Float32 => {
                        if let Value::Float(f) = tag_value {
                            let f = *f;
                            self.builder
                                .field_builder::<Float32Builder>(i)
                                .unwrap()
                                .append_value(f);
                        } else {
                            return Err(ArrowError::InvalidArgumentError(format!(
                                "Invalid tag value {:?} for tag {} a {}",
                                tag_value,
                                tag_name,
                                f.data_type()
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

impl Default for TagsMapBuilder {
    fn default() -> Self {
        Self::new()
    }
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
                            Value::Hex(hex) => {
                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(0)
                                    .unwrap()
                                    .append_value(tag_str);

                                let tag_value_str = std::str::from_utf8(hex.as_ref())?.to_string();
                                tag_struct
                                    .field_builder::<GenericStringBuilder<i32>>(1)
                                    .unwrap()
                                    .append_value(tag_value_str);
                            }
                            Value::Array(arr) => match arr {
                                Array::UInt8(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    let f = arr
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                                Array::Int8(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    let f = arr
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                                Array::Int16(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    let f = arr
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                                Array::UInt16(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    let f = arr
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                                Array::Int32(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    let f = arr
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                                Array::UInt32(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    let f = arr
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                                Array::Float(arr) => {
                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(0)
                                        .unwrap()
                                        .append_value(tag_str);

                                    // CSV the string with two points of precision
                                    let f = arr
                                        .iter()
                                        .map(|v| format!("{:.2}", v))
                                        .collect::<Vec<_>>()
                                        .join(", ");

                                    tag_struct
                                        .field_builder::<GenericStringBuilder<i32>>(1)
                                        .unwrap()
                                        .append_value(f);
                                }
                            },
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
