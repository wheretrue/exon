use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{
        make_builder, ArrayBuilder, ArrayRef, BooleanBuilder, Float32Builder, GenericListArray,
        GenericListBuilder, GenericStringBuilder, Int32Builder, StructArray, StructBuilder,
    },
    datatypes::{DataType, Field, Fields, SchemaRef},
    error::ArrowError,
};
use noodles::vcf::{
    record::{
        genotypes::{
            keys::Key as GenotypeKey, sample::value::Array as GenotypeArray, sample::Value,
        },
        info::field::{value::Array as InfoArray, Key as InfoKey, Value as InfoValue},
        Genotypes, Info,
    },
    Record,
};

/// Builder for the genotypes of a record batch.
struct GenotypeBuilder {
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
                    _ => {
                        return Err(ArrowError::InvalidArgumentError(
                            "format field is not a list of ints or floats".to_string(),
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

                let key = GenotypeKey::from_str(field_name.as_str()).unwrap();
                let value = genotype.get(&key);

                match value {
                    None => match field_type {
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
                    Some(None) => {
                        unimplemented!();
                    }
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
                        Value::Float(float_val) => self
                            .inner
                            .values()
                            .field_builder::<Float32Builder>(i)
                            .expect("expected a float32 builder")
                            .append_value(*float_val),
                        Value::Array(array) => match array {
                            GenotypeArray::Float(float_array) => {
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
                            GenotypeArray::Integer(int_array) => {
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
                            _ => unimplemented!(),
                        },
                        _ => unimplemented!(),
                    },
                }
            }
            self.inner.values().append(true);
        }
        self.inner.append(true);
    }
}

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
                        _ => unimplemented!("{:?}", array),
                    },
                },
            }
        }
        self.inner.append(true);
    }
}

/// A builder for creating a `ArrayRef` from a `VCF` file.
pub struct VCFArrayBuilder {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int32Builder,
    ids: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    references: GenericStringBuilder<i32>,
    alternates: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    qualities: Float32Builder,
    filters: GenericListBuilder<i32, GenericStringBuilder<i32>>,

    infos: InfosBuilder,
    formats: GenotypeBuilder,
}

impl VCFArrayBuilder {
    /// Creates a new `VCFArrayBuilder` from a `Schema`.
    pub fn create(schema: SchemaRef, capacity: usize) -> Result<Self, ArrowError> {
        let info_field = schema.field_with_name("info")?;
        let format_field = schema.field_with_name("formats")?;

        Ok(Self {
            chromosomes: GenericStringBuilder::<i32>::new(),
            positions: Int32Builder::new(),
            ids: GenericListBuilder::<i32, GenericStringBuilder<i32>>::new(GenericStringBuilder::<
                i32,
            >::new()),
            references: GenericStringBuilder::<i32>::new(),
            alternates: GenericListBuilder::<i32, GenericStringBuilder<i32>>::new(
                GenericStringBuilder::<i32>::new(),
            ),
            qualities: Float32Builder::new(),
            filters: GenericListBuilder::<i32, GenericStringBuilder<i32>>::new(
                GenericStringBuilder::<i32>::new(),
            ),

            infos: InfosBuilder::try_new(info_field, capacity)?,

            formats: GenotypeBuilder::try_new(format_field, capacity)?,
        })
    }

    /// Returns the number of records in the builder.
    pub fn len(&self) -> usize {
        self.chromosomes.len()
    }

    /// Appends a record to the builder.
    pub fn append(&mut self, record: &Record) {
        let chromosome: String = format!("{}", record.chromosome());
        self.chromosomes.append_value(chromosome);

        let position: usize = record.position().into();
        self.positions.append_value(position as i32);

        for id in record.ids().iter() {
            self.ids.values().append_value(id.to_string());
        }
        self.ids.append(true);

        let reference: String = format!("{}", record.reference_bases());
        self.references.append_value(reference);

        for alt in record.alternate_bases().iter() {
            self.alternates.values().append_value(alt.to_string());
        }
        self.alternates.append(true);

        let quality = record.quality_score().map(f32::from);
        self.qualities.append_option(quality);

        for filter in record.filters().iter() {
            self.filters.values().append_value(filter.to_string());
        }
        self.filters.append(true);

        self.infos.append_value(record.info());
        self.formats.append_value(record.genotypes());
    }

    /// Builds the `ArrayRef`.
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let chromosomes = self.chromosomes.finish();
        let positions = self.positions.finish();
        let ids = self.ids.finish();
        let references = self.references.finish();
        let alternates = self.alternates.finish();
        let qualities = self.qualities.finish();
        let filters = self.filters.finish();
        let infos = self.infos.finish();
        let formats = self.formats.finish();

        vec![
            Arc::new(chromosomes),
            Arc::new(positions),
            Arc::new(ids),
            Arc::new(references),
            Arc::new(alternates),
            Arc::new(qualities),
            Arc::new(filters),
            Arc::new(infos),
            Arc::new(formats),
        ]
    }
}
