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

use arrow::{
    array::{
        make_builder, ArrayBuilder, BooleanBuilder, Float32Builder, GenericListBuilder,
        GenericStringBuilder, Int32Builder, StructArray, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use noodles::vcf::{
    variant::record::info::field::{value::Array as InfoArray, Value as InfoValue},
    Header,
};

use noodles::vcf::variant::record::Info;

/// Builder for the `infos` field of a `RecordBatch`.
pub struct InfosBuilder {
    inner: StructBuilder,
    fields: Fields,
    header: Arc<Header>,
}

impl InfosBuilder {
    /// Try to create a new `InfosBuilder` from a `Field`.
    pub fn try_new(
        field: &Field,
        header: Arc<Header>,
        capacity: usize,
    ) -> Result<Self, ArrowError> {
        let fields = match field.data_type() {
            DataType::Struct(s) => s,
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "format field is not a struct".to_string(),
                ))
            }
        };

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
                    _ => unimplemented!("unsupported list type"),
                },
                dt => {
                    let builder = make_builder(dt, capacity);
                    builders.push(builder);
                }
            }
        }

        let inner = StructBuilder::new(fields.clone(), builders);

        Ok(Self {
            inner,
            fields: fields.clone(),
            header,
        })
    }

    /// Finish the current array and return it.
    pub fn finish(&mut self) -> StructArray {
        self.inner.finish()
    }

    pub fn append_null(&mut self) {
        for (i, f) in self.fields.iter().enumerate() {
            let field_type = f.data_type();

            match field_type {
                DataType::Int32 => {
                    self.inner
                        .field_builder::<Int32Builder>(i)
                        .unwrap()
                        .append_null();
                }
                DataType::Boolean => {
                    self.inner
                        .field_builder::<BooleanBuilder>(i)
                        .unwrap()
                        .append_null();
                }
                DataType::Float32 => {
                    self.inner
                        .field_builder::<Float32Builder>(i)
                        .unwrap()
                        .append_null();
                }
                DataType::List(l) => match l.data_type() {
                    DataType::Int32 => {
                        let builder = self
                            .inner
                            .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                            .unwrap();

                        builder.append_null();
                    }
                    DataType::Float32 => {
                        let builder = self
                            .inner
                            .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                            .unwrap();

                        builder.append_null();
                    }
                    _ => unimplemented!(),
                },
                _ => unimplemented!("{:?}", field_type),
            }
        }

        self.inner.append_null()
    }

    /// Appends a new value to the end of the builder.
    pub fn append_value<'a>(&mut self, info: Box<dyn Info + 'a>) -> Result<(), ArrowError> {
        for (i, f) in self.fields.iter().enumerate() {
            let field_name = f.name().to_string();
            let field_type = f.data_type();

            let v = info.get(&self.header, field_name.as_str()).transpose()?;

            match v {
                None | Some(None) => match field_type {
                    DataType::Int32 => {
                        self.inner
                            .field_builder::<Int32Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::Utf8 => {
                        self.inner
                            .field_builder::<GenericStringBuilder<i32>>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::Boolean => {
                        self.inner
                            .field_builder::<BooleanBuilder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::Float32 => {
                        self.inner
                            .field_builder::<Float32Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    DataType::List(l) => match l.data_type() {
                        DataType::Int32 => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                                .unwrap();

                            builder.append_null();
                        }
                        DataType::Float32 => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                .unwrap();

                            builder.append_null();
                        }
                        DataType::Utf8 => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                    i,
                                )
                                .unwrap();

                            builder.append_null();
                        }
                        _ => unimplemented!(),
                    },
                    _ => unimplemented!("{:?}", field_type),
                },
                Some(Some(value)) => match value {
                    InfoValue::Integer(int_val) => self
                        .inner
                        .field_builder::<Int32Builder>(i)
                        .unwrap()
                        .append_value(int_val),
                    InfoValue::Float(f) => self
                        .inner
                        .field_builder::<Float32Builder>(i)
                        .unwrap()
                        .append_value(f),
                    InfoValue::String(s) => self
                        .inner
                        .field_builder::<GenericStringBuilder<i32>>(i)
                        .ok_or(ArrowError::InvalidArgumentError(format!(
                            "field {} is not registered as a string, but got {}",
                            field_name, s
                        )))?
                        .append_value(s),
                    InfoValue::Character(c) => self
                        .inner
                        .field_builder::<GenericStringBuilder<i32>>(i)
                        .unwrap()
                        .append_value(c.to_string()),
                    InfoValue::Flag => self
                        .inner
                        .field_builder::<BooleanBuilder>(i)
                        .unwrap()
                        .append_value(true),
                    InfoValue::Array(array) => match array {
                        InfoArray::Integer(int_array) => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                                .unwrap();

                            let builder_values = builder.values();
                            for v in int_array.iter() {
                                let v = v?;
                                builder_values.append_option(v);
                            }
                            builder.append(true);
                        }
                        InfoArray::Float(float_array) => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                .unwrap();

                            let builder_values = builder.values();
                            for v in float_array.iter() {
                                let v = v?;
                                builder_values.append_option(v);
                            }
                            builder.append(true);
                        }
                        InfoArray::String(string_array) => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                    i,
                                )
                                .unwrap();

                            let builder_values = builder.values();
                            for v in string_array.iter() {
                                let v = v?;
                                builder_values.append_option(v);
                            }
                            builder.append(true);
                        }
                        InfoArray::Character(char_array) => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                    i,
                                )
                                .unwrap();

                            let builder_values = builder.values();
                            for v in char_array.iter() {
                                let v = v?.map(|c| c.to_string());
                                builder_values.append_option(v);
                            }
                            builder.append(true);
                        }
                    },
                },
            }
        }
        self.inner.append(true);

        Ok(())
    }
}
