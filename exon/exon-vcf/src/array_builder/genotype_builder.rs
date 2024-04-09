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

use arrow::{
    array::{
        make_builder, ArrayBuilder, Float32Builder, GenericListArray, GenericListBuilder,
        GenericStringBuilder, Int32Builder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use noodles::vcf::{
    variant::record::samples::{
        series::{value::Array, Value},
        Sample,
    },
    variant::record::Samples as VCFSamples,
    Header,
};

/// Builder for the genotypes of a record batch.
pub struct GenotypeBuilder {
    inner: GenericListBuilder<i32, StructBuilder>,
    fields: Fields,
}

impl GenotypeBuilder {
    pub fn try_new(field: &Field, capacity: usize) -> Result<Self, ArrowError> {
        let fields = match field.data_type() {
            DataType::List(s) => match s.data_type() {
                DataType::Struct(s) => s,
                _ => {
                    return Err(ArrowError::InvalidArgumentError(
                        "format field is not a struct".to_string(),
                    ))
                }
            },
            _ => {
                return Err(ArrowError::InvalidArgumentError(
                    "format field is not a list".to_string(),
                ))
            }
        };

        let mut builders = Vec::<Box<dyn ArrayBuilder>>::new();

        for field in fields.clone().iter() {
            match field.data_type() {
                DataType::List(itype) => match itype.data_type() {
                    DataType::Int32 => {
                        let builder = GenericListBuilder::<i32, Int32Builder>::with_capacity(
                            Int32Builder::new(),
                            capacity,
                        );

                        builders.push(Box::new(builder));
                    }
                    DataType::Float32 => {
                        let builder = GenericListBuilder::<i32, Float32Builder>::with_capacity(
                            Float32Builder::new(),
                            capacity,
                        );

                        builders.push(Box::new(builder));
                    }
                    DataType::Utf8 => {
                        let builder =
                            GenericListBuilder::<i32, GenericStringBuilder<i32>>::with_capacity(
                                GenericStringBuilder::<i32>::new(),
                                capacity,
                            );

                        builders.push(Box::new(builder));
                    }
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(
                            "format field is not a value type".to_string(),
                        ))
                    }
                },
                dt => {
                    let builder = make_builder(dt, capacity);
                    builders.push(builder);
                }
            }
        }

        let inner_struct = StructBuilder::new(fields.clone(), builders);
        let inner = GenericListBuilder::<i32, StructBuilder>::new(inner_struct);

        Ok(Self {
            inner,
            fields: fields.clone(),
        })
    }

    pub fn finish(&mut self) -> GenericListArray<i32> {
        self.inner.finish()
    }

    pub fn append_null(&mut self) {
        self.inner.append(false);
    }

    /// Appends a record to the builder.
    ///
    /// It is important that the passed genotypes was parsed using the same header as the one used
    /// to create this builder. If not, some types may not match and the append will fail.
    pub fn append_value<'a>(
        &mut self,
        samples: Box<dyn VCFSamples + 'a>,
        header: &Header,
    ) -> Result<(), ArrowError> {
        for sample in samples.iter() {
            for (i, field) in self.fields.clone().iter().enumerate() {
                let field_name = field.name().to_string();
                let field_type = field.data_type();

                let value = sample.get(header, &field_name).transpose()?;

                match value {
                    None | Some(None) => match field_type {
                        DataType::Int32 => {
                            self.inner
                                .values()
                                .field_builder::<Int32Builder>(i)
                                .expect("expected an int32 builder")
                                .append_null();
                        }
                        DataType::Utf8 => {
                            self.inner
                                .values()
                                .field_builder::<GenericStringBuilder<i32>>(i)
                                .expect("expected a string builder")
                                .append_null();
                        }
                        DataType::List(inner_type) => match inner_type.data_type() {
                            DataType::Int32 => {
                                let vs = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                                    .expect("expected a list builder");

                                vs.append_null();
                            }
                            DataType::Float32 => {
                                let vs = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                    .expect("expected a list builder");

                                vs.append_null();
                            }
                            DataType::Utf8 => {
                                let vs = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                        i,
                                    )
                                    .expect("expected a list builder");

                                vs.append_null();
                            }
                            _ => unimplemented!(),
                        },
                        DataType::Float32 => {
                            self.inner
                                .values()
                                .field_builder::<Float32Builder>(i)
                                .expect("expected a float32 builder")
                                .append_null();
                        }
                        _ => unimplemented!(),
                    },
                    Some(Some(value)) => match value {
                        Value::Integer(int_val) => self
                            .inner
                            .values()
                            .field_builder::<Int32Builder>(i)
                            .expect("expected an int32 builder")
                            .append_value(int_val),
                        Value::String(string_val) => self
                            .inner
                            .values()
                            .field_builder::<GenericStringBuilder<i32>>(i)
                            .expect("expected a string builder")
                            .append_value(string_val),
                        Value::Character(char_val) => self
                            .inner
                            .values()
                            .field_builder::<GenericStringBuilder<i32>>(i)
                            .expect("expected a string builder")
                            .append_value(&char_val.to_string()),
                        Value::Float(float_val) => self
                            .inner
                            .values()
                            .field_builder::<Float32Builder>(i)
                            .expect("expected a float32 builder")
                            .append_value(float_val),
                        Value::Genotype(_gt) => {
                            todo!("genotype not implemented")
                        }
                        Value::Array(array) => match array {
                            Array::Float(float_array) => {
                                let builder = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                    .expect("expected a list builder");

                                let builder_values = builder.values();
                                for v in float_array.iter() {
                                    let v = v?;
                                    builder_values.append_option(v);
                                }
                                builder.append(true);
                            }
                            Array::Integer(int_array) => {
                                let builder = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                                    .expect("expected a list builder");

                                let builder_values = builder.values();
                                for v in int_array.iter() {
                                    let v = v?;
                                    builder_values.append_option(v);
                                }
                                builder.append(true);
                            }
                            Array::Character(char_array) => {
                                let builder = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                        i,
                                    )
                                    .expect("expected a list builder");

                                let builder_values = builder.values();
                                for v in char_array.iter() {
                                    let v = v?;
                                    builder_values.append_option(v.as_ref().map(|c| c.to_string()));
                                }
                                builder.append(true);
                            }
                            Array::String(string_array) => {
                                let builder = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                        i,
                                    )
                                    .expect("expected a list builder");

                                let builder_values = builder.values();
                                for v in string_array.iter() {
                                    let v = v?;
                                    builder_values.append_option(v);
                                }
                                builder.append(true);
                            }
                        },
                    },
                }
            }
            self.inner.values().append(true);
        }
        self.inner.append(true);

        Ok(())
    }
}
