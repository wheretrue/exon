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

    pub fn append_value(&mut self, genotypes: &Genotypes) {
        for genotype in genotypes.values() {
            for (i, field) in self.fields.clone().iter().enumerate() {
                let field_name = field.name().to_string();
                let field_type = field.data_type();

                let key = Key::from_str(field_name.as_str()).unwrap();
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

                                vs.values().append_null();
                                vs.append(true);
                            }
                            DataType::Float32 => {
                                let vs = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                    .expect("expected a list builder");

                                vs.values().append_null();
                                vs.append(true);
                            }
                            DataType::Utf8 => {
                                let vs = self
                                    .inner
                                    .values()
                                    .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                        i,
                                    )
                                    .expect("expected a list builder");

                                vs.values().append_null();
                                vs.append(true);
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
    use noodles::vcf::record::{
        genotypes::{keys::Key, sample::Value, Keys},
        Genotypes,
    };

    use noodles::vcf::record::genotypes::sample::value::Array as VcfArray;

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

    fn test_builder_scalar_path() {
        let fields = vec![
            Field::new("GT", arrow::datatypes::DataType::Utf8, false),
            Field::new("DP", arrow::datatypes::DataType::Int32, false),
            Field::new("CNQ", arrow::datatypes::DataType::Float32, false),
            Field::new("AA", arrow::datatypes::DataType::Utf8, false),
            Field::new("CC", arrow::datatypes::DataType::Utf8, false),
            Field::new("AB", arrow::datatypes::DataType::Int32, false),
            Field::new("AC", arrow::datatypes::DataType::Float32, false),
            Field::new(
                "LI",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Int32,
                    false,
                ))),
                false,
            ),
            Field::new(
                "LS",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ))),
                false,
            ),
            Field::new(
                "LF",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Float32,
                    false,
                ))),
                false,
            ),
            Field::new(
                "LC",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ))),
                false,
            ),
            // Missing lists
            Field::new(
                "MLI",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Int32,
                    false,
                ))),
                false,
            ),
            Field::new(
                "MLS",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Utf8,
                    false,
                ))),
                false,
            ),
            Field::new(
                "MLF",
                arrow::datatypes::DataType::List(Arc::new(Field::new(
                    "item",
                    arrow::datatypes::DataType::Float32,
                    false,
                ))),
                false,
            ),
        ];

        let inner_struct_type = arrow::datatypes::DataType::Struct(Fields::from(fields));
        let inner_struct = Field::new("formats", inner_struct_type, false);

        let inner_list_type = arrow::datatypes::DataType::List(Arc::new(inner_struct));
        let field = Field::new("formats", inner_list_type, false);

        let mut gb = GenotypeBuilder::try_new(&field, 0).unwrap();

        let genotypes = Genotypes::new(
            Keys::try_from(vec![
                Key::from_str("GT").unwrap(),
                Key::from_str("DP").unwrap(),
                Key::from_str("CNQ").unwrap(),
                Key::from_str("AA").unwrap(),
                Key::from_str("AB").unwrap(),
                Key::from_str("AC").unwrap(),
                Key::from_str("CC").unwrap(),
                Key::from_str("LI").unwrap(),
                Key::from_str("LS").unwrap(),
                Key::from_str("LF").unwrap(),
                Key::from_str("LC").unwrap(),
            ])
            .unwrap(),
            vec![vec![
                Some(Value::String("1/1".to_string())),
                Some(Value::Integer(0)),
                Some(Value::Float(0.0)),
                None,
                None,
                None,
                Some(Value::Character('A')),
                Some(Value::Array(VcfArray::Integer(vec![Some(51), Some(51)]))),
                Some(Value::Array(VcfArray::String(vec![Some(
                    "1/1".to_string(),
                )]))),
                Some(Value::Array(VcfArray::Float(vec![Some(0.0), Some(0.0)]))),
                Some(Value::Array(VcfArray::Character(vec![
                    Some('A'),
                    Some('A'),
                ]))),
            ]],
        );

        gb.append_value(&genotypes);

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
+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| test                                                                                                                                                          |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------+
| [{GT: 1/1, DP: 0, CNQ: 0.0, AA: NULL, CC: A, AB: NULL, AC: NULL, LI: [51, 51], LS: [1/1], LF: [0.0, 0.0], LC: [A, A], MLI: [NULL], MLS: [NULL], MLF: [NULL]}] |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------+";

        assert_eq!(formatted.to_string(), expected);
    }
}
