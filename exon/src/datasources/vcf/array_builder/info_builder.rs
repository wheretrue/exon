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

    /// Appends a new value to the end of the builder.
    pub fn append_value(&mut self, info: &Info) {
        for (i, f) in self.fields.iter().enumerate() {
            let field_name = f.name().to_string();
            let field_type = f.data_type();

            let key = InfoKey::from_str(field_name.as_str()).unwrap();

            let v = info.get(&key);

            match v {
                None => match field_type {
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
                    DataType::List(l) => match l.data_type() {
                        DataType::Int32 => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Int32Builder>>(i)
                                .unwrap();

                            builder.values().append_null();

                            builder.append(true);
                        }
                        DataType::Float32 => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, Float32Builder>>(i)
                                .unwrap();

                            builder.values().append_null();

                            builder.append(true);
                        }
                        DataType::Utf8 => {
                            let builder = self
                                .inner
                                .field_builder::<GenericListBuilder<i32, GenericStringBuilder<i32>>>(
                                    i,
                                )
                                .unwrap();

                            builder.values().append_null();

                            builder.append(true);
                        }
                        _ => unimplemented!(),
                    },
                    DataType::Float32 => {
                        self.inner
                            .field_builder::<Float32Builder>(i)
                            .unwrap()
                            .append_null();
                    }
                    _ => unimplemented!("{:?}", field_type),
                },
                Some(None) => {
                    unimplemented!();
                }
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
                        _ => unimplemented!("{:?}", array),
                    },
                },
            }
        }
        self.inner.append(true);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        datatypes::{DataType, Field, Fields},
        util::{display::FormatOptions, pretty::pretty_format_columns_with_options},
    };
    use noodles::vcf::record::Info;

    use super::InfosBuilder;

    #[test]
    fn test_bad_init_genotype() {
        let fields = vec![Field::new("GT", DataType::Int32, false)];

        let struct_type = DataType::Struct(Fields::from(fields));
        let info_field = Field::new("INFO", struct_type, false);

        let mut ib = InfosBuilder::try_new(&info_field, 1).unwrap();

        let infos = Info::default();

        ib.append_value(&infos);

        let ib_array = Arc::new(ib.finish());

        let options = FormatOptions::default().with_null("NULL");
        let formatted = pretty_format_columns_with_options("test", &[ib_array], &options).unwrap();

        let expected = "\
+------------+
| test       |
+------------+
| {GT: NULL} |
+------------+";

        assert_eq!(formatted.to_string(), expected);
    }
}
