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

use std::{str::FromStr, sync::Arc};

use arrow::{
    array::{ArrayRef, Float32Builder, GenericListBuilder, GenericStringBuilder, Int64Builder},
    datatypes::{DataType, SchemaRef},
    error::ArrowError,
};
use exon_common::ExonArrayBuilder;
use noodles::vcf::{
    record::{AlternateBases, Chromosome, Genotypes, Info, Position, ReferenceBases},
    Header,
};

use super::{GenotypeBuilder, InfosBuilder};

enum InfosFormat {
    Struct(InfosBuilder),
    String(GenericStringBuilder<i32>),
}

enum FormatsFormat {
    List(GenotypeBuilder),
    String(GenericStringBuilder<i32>),
}

/// A builder for creating a `ArrayRef` from a `VCF` file.
pub struct LazyVCFArrayBuilder {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int64Builder,
    ids: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    references: GenericStringBuilder<i32>,
    alternates: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    qualities: Float32Builder,
    filters: GenericListBuilder<i32, GenericStringBuilder<i32>>,

    infos: InfosFormat,
    formats: FormatsFormat,
    projection: Vec<usize>,

    header: Arc<Header>,

    rows: usize,
}

impl LazyVCFArrayBuilder {
    /// Creates a new `VCFArrayBuilder` from a `Schema`.
    pub fn create(
        schema: SchemaRef,
        capacity: usize,
        projection: Option<Vec<usize>>,
        header: Arc<Header>,
    ) -> Result<Self, ArrowError> {
        let info_field = schema.field_with_name("info")?;

        let item_capacity = capacity;
        let data_capacity = capacity * 8;

        let infos = match &info_field.data_type() {
            DataType::Utf8 => InfosFormat::String(GenericStringBuilder::<i32>::with_capacity(
                item_capacity,
                data_capacity,
            )),
            DataType::Struct(_) => {
                InfosFormat::Struct(InfosBuilder::try_new(info_field, capacity)?)
            }
            _ => {
                return Err(ArrowError::SchemaError(
                    format!("Unexpected type for VCF file: {:?}", info_field.data_type())
                        .to_string(),
                ))
            }
        };

        let format_field = schema.field_with_name("formats")?;

        let formats = match &format_field.data_type() {
            DataType::Utf8 => FormatsFormat::String(GenericStringBuilder::<i32>::with_capacity(
                item_capacity,
                data_capacity,
            )),
            DataType::List(_) => {
                FormatsFormat::List(GenotypeBuilder::try_new(format_field, capacity)?)
            }
            _ => {
                return Err(ArrowError::SchemaError(
                    format!(
                        "Unexpected type for VCF file: {:?}",
                        format_field.data_type()
                    )
                    .to_string(),
                ))
            }
        };

        let projection = match projection {
            Some(projection) => projection,
            None => (0..schema.fields().len()).collect(),
        };

        Ok(Self {
            chromosomes: GenericStringBuilder::<i32>::with_capacity(item_capacity, data_capacity),
            positions: Int64Builder::with_capacity(item_capacity),
            ids: GenericListBuilder::<i32, GenericStringBuilder<i32>>::with_capacity(
                GenericStringBuilder::<i32>::new(),
                capacity,
            ),
            references: GenericStringBuilder::<i32>::with_capacity(item_capacity, data_capacity),
            alternates: GenericListBuilder::<i32, GenericStringBuilder<i32>>::with_capacity(
                GenericStringBuilder::<i32>::new(),
                capacity,
            ),
            qualities: Float32Builder::with_capacity(capacity),
            filters: GenericListBuilder::<i32, GenericStringBuilder<i32>>::with_capacity(
                GenericStringBuilder::<i32>::new(),
                capacity,
            ),

            infos,
            formats,

            projection,

            header,

            rows: 0,
        })
    }

    /// Appends a record to the builder.
    pub fn append(&mut self, record: &noodles::vcf::lazy::Record) -> Result<(), ArrowError> {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let chromosome = Chromosome::from_str(record.chromosome()).map_err(|_| {
                        ArrowError::ParseError(format!(
                            "Could not parse chromosome: {}",
                            record.chromosome()
                        ))
                    })?;

                    self.chromosomes.append_value(chromosome.to_string());
                }
                1 => {
                    let position = Position::from_str(record.position()).map_err(|_| {
                        ArrowError::ParseError(format!(
                            "Could not parse position: {}",
                            record.position()
                        ))
                    })?;

                    let pos_usize: usize = position.into();
                    self.positions.append_value(pos_usize as i64);
                }
                2 => {
                    if record.ids().is_empty() {
                        self.ids.append_null();
                    } else {
                        for id in record.ids().iter() {
                            self.ids.values().append_value(id);
                        }

                        self.ids.append(true);
                    }
                }
                3 => {
                    let reference_bases = ReferenceBases::from_str(record.reference_bases())
                        .map_err(|_| {
                            ArrowError::ParseError("Invalid reference bases".to_string())
                        })?;

                    self.references.append_value(reference_bases.to_string());
                }
                4 => match record.alternate_bases() {
                    "." => self.alternates.append_null(),
                    _ => {
                        let alternate_bases = AlternateBases::from_str(record.alternate_bases())
                            .map_err(|_| {
                                ArrowError::ParseError(format!(
                                    "Invalid alternate bases: {}",
                                    record.alternate_bases()
                                ))
                            })?;

                        for alt in alternate_bases.iter() {
                            self.alternates.values().append_value(alt.to_string());
                        }

                        self.alternates.append(true);
                    }
                },
                5 => {
                    let qs = record.quality_score();

                    match qs {
                        Some(qs) => match qs {
                            "." => self.qualities.append_null(),
                            _ => {
                                let parsed_qs = qs.parse::<f32>().map_err(|_| {
                                    ArrowError::ParseError(format!(
                                        "Could not parse quality score: {qs}",
                                        qs = qs
                                    ))
                                })?;

                                self.qualities.append_value(parsed_qs);
                            }
                        },
                        None => self.qualities.append_null(),
                    }
                }
                6 => match record.filters() {
                    Some(f) => {
                        for filter in f.iter() {
                            self.filters.values().append_value(filter);
                        }
                        self.filters.append(true);
                    }
                    None => {
                        self.filters.append_null();
                    }
                },
                7 => match self.infos {
                    InfosFormat::String(ref mut builder) => {
                        builder.append_value(record.info().as_ref());
                    }
                    InfosFormat::Struct(ref mut builder) => match record.info().as_ref() {
                        "." => builder.append_null(),
                        _ => {
                            let infos = Info::from_str(record.info().as_ref()).map_err(|_| {
                                ArrowError::ParseError(format!(
                                    "Invalid info {}",
                                    record.info().as_ref()
                                ))
                            })?;

                            builder.append_value(&infos)?;
                        }
                    },
                },
                8 => match self.formats {
                    FormatsFormat::String(ref mut builder) => {
                        builder.append_value(record.genotypes().as_ref());
                    }
                    FormatsFormat::List(ref mut builder) => {
                        let genotypes = Genotypes::parse(record.genotypes().as_ref(), &self.header)
                            .map_err(|_| {
                                ArrowError::ParseError(format!(
                                    "Invalid genotypes {}",
                                    record.genotypes().as_ref()
                                ))
                            })?;

                        builder.append_value(&genotypes)?;
                    }
                },
                _ => {
                    return Err(ArrowError::SchemaError(
                        "Unexpected number of columns for VCF file".to_string(),
                    ))
                }
            }
        }

        self.rows += 1;

        Ok(())
    }

    /// Builds the `ArrayRef`.
    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let mut arrays: Vec<ArrayRef> = vec![];

        for col_idx in self.projection.iter() {
            match col_idx {
                0 => arrays.push(Arc::new(self.chromosomes.finish())),
                1 => arrays.push(Arc::new(self.positions.finish())),
                2 => arrays.push(Arc::new(self.ids.finish())),
                3 => arrays.push(Arc::new(self.references.finish())),
                4 => arrays.push(Arc::new(self.alternates.finish())),
                5 => arrays.push(Arc::new(self.qualities.finish())),
                6 => arrays.push(Arc::new(self.filters.finish())),
                7 => match self.infos {
                    InfosFormat::String(ref mut builder) => {
                        arrays.push(Arc::new(builder.finish()));
                    }
                    InfosFormat::Struct(ref mut builder) => {
                        arrays.push(Arc::new(builder.finish()));
                    }
                },
                8 => match self.formats {
                    FormatsFormat::String(ref mut builder) => {
                        arrays.push(Arc::new(builder.finish()));
                    }
                    FormatsFormat::List(ref mut builder) => {
                        arrays.push(Arc::new(builder.finish()));
                    }
                },
                _ => panic!("Not implemented"),
            }
        }

        arrays
    }
}

impl ExonArrayBuilder for LazyVCFArrayBuilder {
    fn len(&self) -> usize {
        self.rows
    }

    fn finish(&mut self) -> Vec<ArrayRef> {
        self.finish()
    }
}
