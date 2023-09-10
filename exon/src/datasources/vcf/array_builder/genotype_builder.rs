use std::str::FromStr;

use arrow::{
    array::{
        make_builder, ArrayBuilder, Float32Builder, GenericListArray, GenericListBuilder,
        GenericStringBuilder, Int32Builder, StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
    error::ArrowError,
};
use noodles::vcf::record::{
    genotypes::{
        keys::Key,
        sample::{value::Array, Value},
    },
    Genotypes,
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

    /// Appends a record to the builder.
    ///
    /// It is important that the passed genotypes was parsed using the same header as the one used
    /// to create this builder. If not, some types may not match and the append will fail.
    pub fn append_value(&mut self, genotypes: &Genotypes) -> Result<(), ArrowError> {
        for genotype in genotypes.values() {
            for (i, field) in self.fields.clone().iter().enumerate() {
                let field_name = field.name().to_string();
                let field_type = field.data_type();

                let key = Key::from_str(field_name.as_str()).map_err(|_| {
                    ArrowError::InvalidArgumentError(format!(
                        "invalid field name: {}",
                        field_name.as_str()
                    ))
                })?;
                let value = genotype.get(&key);

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
                            .append_value(*int_val),
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
                            .append_value(*float_val),
                        Value::Array(array) => match array {
                            Array::Float(float_array) => {
                                let builder = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                    .expect("expected a list builder");

                                let builder_values = builder.values();
                                for v in float_array {
                                    builder_values.append_option(*v);
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
                                for v in int_array {
                                    builder_values.append_option(*v);
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
                                for v in char_array {
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
                                for v in string_array {
                                    builder_values.append_option(v.clone());
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

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use arrow::{
        array::Array,
        datatypes::{Field, Fields},
        util::{display::FormatOptions, pretty::pretty_format_columns_with_options},
    };
    use noodles::vcf::{
        header::{
            record::value::{map::format, Map},
            Number,
        },
        record::{
            genotypes::{self, sample::Value, Keys},
            Genotypes,
        },
        Header,
    };

    use crate::datasources::vcf::array_builder::genotype_builder::GenotypeBuilder;

    #[test]
    fn test_bad_init_genotype() {
        let bad_field = Field::new("formats", arrow::datatypes::DataType::Int32, false);

        let gb = GenotypeBuilder::try_new(&bad_field, 0);
        assert!(gb.is_err());

        let list_bad_field = Field::new(
            "formats",
            arrow::datatypes::DataType::List(Arc::new(bad_field)),
            false,
        );

        let gb = GenotypeBuilder::try_new(&list_bad_field, 0);
        assert!(gb.is_err());
    }

    #[test]
    fn test_builder() {
        let mut header_builder = Header::builder();
        let mut expected_fields = Vec::new();

        let test_table = vec![
            (
                "single_int",
                Number::Count(1),
                format::Type::Integer,
                arrow::datatypes::Field::new("single_int", arrow::datatypes::DataType::Int32, true),
                "1",
            ),
            (
                "single_float",
                Number::Count(1),
                format::Type::Float,
                arrow::datatypes::Field::new(
                    "single_float",
                    arrow::datatypes::DataType::Float32,
                    true,
                ),
                "1.0",
            ),
            (
                "single_char",
                Number::Count(1),
                format::Type::Character,
                arrow::datatypes::Field::new("single_char", arrow::datatypes::DataType::Utf8, true),
                "a",
            ),
            (
                "single_string",
                Number::Count(1),
                format::Type::String,
                arrow::datatypes::Field::new(
                    "single_string",
                    arrow::datatypes::DataType::Utf8,
                    true,
                ),
                "a",
            ),
            (
                "single_int_array",
                Number::Count(2),
                format::Type::Integer,
                arrow::datatypes::Field::new(
                    "single_int_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Int32,
                        true,
                    ))),
                    true,
                ),
                "1",
            ),
            (
                "single_float_array",
                Number::Count(2),
                format::Type::Float,
                arrow::datatypes::Field::new(
                    "single_float_array",
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
                "single_char_array",
                Number::Count(2),
                format::Type::Character,
                arrow::datatypes::Field::new(
                    "single_char_array",
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
                "single_string_array",
                Number::Count(2),
                format::Type::String,
                arrow::datatypes::Field::new(
                    "single_string_array",
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
                "missing_string_array",
                Number::Count(2),
                format::Type::String,
                arrow::datatypes::Field::new(
                    "missing_string_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
                ".",
            ),
            (
                "missing_float_array",
                Number::Count(2),
                format::Type::Float,
                arrow::datatypes::Field::new(
                    "missing_float_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Float32,
                        true,
                    ))),
                    true,
                ),
                ".",
            ),
            (
                "missing_int_array",
                Number::Count(2),
                format::Type::Integer,
                arrow::datatypes::Field::new(
                    "missing_int_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Int32,
                        true,
                    ))),
                    true,
                ),
                ".",
            ),
            (
                "missing_string",
                Number::Count(1),
                format::Type::String,
                arrow::datatypes::Field::new(
                    "missing_string",
                    arrow::datatypes::DataType::Utf8,
                    true,
                ),
                ".",
            ),
        ];

        let mut keys = Vec::new();
        let mut values = Vec::new();

        for (a, b, c, d, e) in test_table {
            let key = genotypes::keys::Key::from_str(a).unwrap();

            keys.push(genotypes::keys::Key::from_str(a).unwrap());

            let format = Map::builder()
                .set_description("test")
                .set_number(b)
                .set_type(c)
                .set_idx(1)
                .build()
                .unwrap();

            if e == "." {
                values.push(None);
            } else {
                let value = Value::try_from((b, c, e)).unwrap();
                values.push(Some(value));
            }

            header_builder = header_builder.add_format(key, format);

            expected_fields.push(d);
        }

        let header = header_builder.build();
        let genotypes = Genotypes::new(Keys::try_from(keys).unwrap(), vec![values]);

        let gt_string = genotypes.to_string();

        let gt = Genotypes::parse(&gt_string, &header).unwrap();

        let field = Field::new(
            "formats",
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::Struct(Fields::from(expected_fields)),
                false,
            ))),
            false,
        );
        let mut gb = GenotypeBuilder::try_new(&field, 0).unwrap();

        gb.append_value(&gt).unwrap();

        let array = Arc::new(gb.finish());

        assert_eq!(array.len(), 1);
        assert_eq!(array.null_count(), 0);

        let formatted = pretty_format_columns_with_options(
            "test",
            &[array],
            &FormatOptions::default().with_null("NULL"),
        )
        .unwrap();

        let expected = "\
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| test                                                                                                                                                                                                                                                                                        |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| [{single_int: 1, single_float: 1.0, single_char: a, single_string: a, single_int_array: [1], single_float_array: [1.0, 2.0], single_char_array: [a, b], single_string_array: [a, b], missing_string_array: NULL, missing_float_array: NULL, missing_int_array: NULL, missing_string: NULL}] |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+";

        assert_eq!(formatted.to_string(), expected);
    }
}
