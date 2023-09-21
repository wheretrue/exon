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

use std::str::FromStr;

use arrow::{
    array::{
        make_builder, ArrayBuilder, BooleanBuilder, Float32Builder, GenericListBuilder,
        GenericStringBuilder, Int32Builder, StructArray, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use noodles::vcf::record::{
    info::field::{value::Array as InfoArray, Key as InfoKey, Value as InfoValue},
    Info,
};

/// Builder for the `infos` field of a `RecordBatch`.
pub struct InfosBuilder {
    inner: StructBuilder,
    fields: Fields,
}

impl InfosBuilder {
    /// Try to create a new `InfosBuilder` from a `Field`.
    pub fn try_new(field: &Field, capacity: usize) -> Result<Self, ArrowError> {
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
    pub fn append_value(&mut self, info: &Info) {
        for (i, f) in self.fields.iter().enumerate() {
            let field_name = f.name().to_string();
            let field_type = f.data_type();

            let key = InfoKey::from_str(field_name.as_str()).unwrap();

            let v = info.get(&key);

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
                        .append_value(*int_val),
                    InfoValue::Float(f) => self
                        .inner
                        .field_builder::<Float32Builder>(i)
                        .unwrap()
                        .append_value(*f),
                    InfoValue::String(s) => self
                        .inner
                        .field_builder::<GenericStringBuilder<i32>>(i)
                        .unwrap()
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
                            for v in int_array {
                                builder_values.append_option(*v);
                            }
                            builder.append(true);
                        }
                        InfoArray::Float(float_array) => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                .unwrap();

                            let builder_values = builder.values();
                            for v in float_array {
                                builder_values.append_option(*v);
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
                            for v in string_array {
                                builder_values.append_option(v.clone());
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
                            for v in char_array {
                                let v = v.map(|c| c.to_string());
                                builder_values.append_option(v);
                            }
                            builder.append(true);
                        }
                    },
                },
            }
        }
        self.inner.append(true);
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use arrow::{
        array::Array,
        util::{display::FormatOptions, pretty::pretty_format_columns_with_options},
    };
    use noodles::vcf::{
        header::{
            record::value::{map::info, Map},
            Number,
        },
        record::{
            info::field::{Key, Value},
            Info,
        },
        Header,
    };

    use super::InfosBuilder;

    #[test]
    fn test_vcf_builder() {
        let info_test_table = vec![
            (
                "single_int",
                Number::Count(1),
                info::Type::Integer,
                arrow::datatypes::Field::new("single_int", arrow::datatypes::DataType::Int32, true),
                "1",
            ),
            (
                "single_str",
                Number::Count(1),
                info::Type::String,
                arrow::datatypes::Field::new("single_str", arrow::datatypes::DataType::Utf8, true),
                "str",
            ),
            (
                "single_flag",
                Number::Count(0),
                info::Type::Flag,
                arrow::datatypes::Field::new(
                    "single_flag",
                    arrow::datatypes::DataType::Boolean,
                    true,
                ),
                "",
            ),
            (
                "single_char",
                Number::Count(1),
                info::Type::Character,
                arrow::datatypes::Field::new("single_char", arrow::datatypes::DataType::Utf8, true),
                "a",
            ),
            (
                "single_float",
                Number::Count(1),
                info::Type::Float,
                arrow::datatypes::Field::new(
                    "single_float",
                    arrow::datatypes::DataType::Float32,
                    true,
                ),
                "1.0",
            ),
            (
                "array_int",
                Number::Count(2),
                info::Type::Integer,
                arrow::datatypes::Field::new(
                    "array_int",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Int32,
                        true,
                    ))),
                    true,
                ),
                "1,2",
            ),
            (
                "array_str",
                Number::Count(2),
                info::Type::String,
                arrow::datatypes::Field::new(
                    "array_str",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
                "str1,str2",
            ),
            (
                "array_char",
                Number::Count(2),
                info::Type::Character,
                arrow::datatypes::Field::new(
                    "array_char",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
                "a,b",
            ),
            (
                "array_float",
                Number::Count(2),
                info::Type::Float,
                arrow::datatypes::Field::new(
                    "array_float",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Float32,
                        true,
                    ))),
                    true,
                ),
                "1.0,2.0",
            ),
            (
                "missing_array_int",
                Number::Count(2),
                info::Type::Integer,
                arrow::datatypes::Field::new(
                    "missing_array_int",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Int32,
                        true,
                    ))),
                    true,
                ),
                ".",
            ),
        ];

        let mut fields = Vec::new();
        let mut header = Header::builder();

        let mut info_val = Info::default();

        for (key_str, number, ty, field, val) in info_test_table {
            let key = Key::from_str(key_str).unwrap();
            let info = Map::builder()
                .set_description(key_str)
                .set_number(number)
                .set_type(ty)
                .build()
                .unwrap();

            header = header.add_info(key, info.clone());
            fields.push(field);

            let key2 = Key::from_str(key_str).unwrap();
            if val == "." {
                info_val.insert(key2, None);
            } else {
                let value = Value::try_from((number, ty, val)).unwrap();
                info_val.insert(key2, Some(value));
            }
        }

        let header = header.build();

        let info_string = info_val.to_string();
        let info = Info::try_from_str(&info_string, header.infos()).unwrap();

        let dt = arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(fields));
        let field = arrow::datatypes::Field::new("info", dt, false);

        let mut ib = InfosBuilder::try_new(&field, 0).unwrap();

        ib.append_value(&info);

        let array = Arc::new(ib.finish());

        assert_eq!(array.len(), 1);

        let formatted = pretty_format_columns_with_options(
            "test",
            &[array],
            &FormatOptions::default().with_null("NULL"),
        )
        .unwrap();

        let expected = "\
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| test                                                                                                                                                                                                     |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| {single_int: 1, single_str: str, single_flag: true, single_char: a, single_float: 1.0, array_int: [1, 2], array_str: [str1, str2], array_char: [a, b], array_float: [1.0, 2.0], missing_array_int: NULL} |
+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+";

        assert_eq!(formatted.to_string(), expected);
    }
}
