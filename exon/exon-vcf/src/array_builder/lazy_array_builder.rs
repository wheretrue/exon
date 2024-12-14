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
    array::{ArrayRef, Float32Builder, GenericListBuilder, GenericStringBuilder, Int64Builder},
    datatypes::{DataType, SchemaRef},
    error::ArrowError,
};
use exon_common::ExonArrayBuilder;
use noodles::vcf::{
    variant::record::{
        info::field::{value::Array as InfosArray, Value as InfosValue},
        samples::series::{
            value::{genotype::Phasing, Array},
            Value as SamplesValue,
        },
        Filters, Ids, Info, Samples,
    },
    Header,
};

use noodles::vcf::variant::record::AlternateBases;

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
                InfosFormat::Struct(InfosBuilder::try_new(info_field, header.clone(), capacity)?)
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
    pub fn append<T>(&mut self, record: T) -> Result<(), ArrowError>
    where
        T: noodles::vcf::variant::record::Record,
    {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    self.chromosomes
                        .append_value(record.reference_sequence_name(&self.header)?);
                }
                1 => {
                    let variant_start = record.variant_start().transpose()?;

                    self.positions
                        .append_option(variant_start.map(|v| v.get() as i64));
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
                    let reference_bases = record.reference_bases();

                    let mut s = String::new();
                    for base in reference_bases.iter() {
                        s.push(base?.into());
                    }

                    self.references.append_value(s);
                }
                4 => {
                    let alt_bases = record.alternate_bases();

                    if alt_bases.is_empty() {
                        self.alternates.append_null();
                    } else {
                        let mut s = String::new();
                        for alt in alt_bases.iter() {
                            let alt = alt?;
                            s += alt;
                        }

                        self.alternates.append(true);
                    }
                }
                5 => {
                    let qs = record.quality_score().transpose()?;
                    self.qualities.append_option(qs);
                }
                6 => {
                    for filter in record.filters().iter(&self.header) {
                        let filter = filter?;
                        self.filters.values().append_value(filter);
                    }

                    self.filters.append(true);
                }
                7 => match self.infos {
                    InfosFormat::String(ref mut builder) => {
                        let info = record.info();

                        let mut vs = Vec::new();
                        for i in info.iter(&self.header) {
                            let (key, value_option) = i?;
                            let value = value_option.unwrap();

                            let mut s = String::new();

                            s.push_str(key);
                            s.push('=');

                            match value {
                                InfosValue::String(v) => s.push_str(&v),
                                InfosValue::Character(v) => s.push(v),
                                InfosValue::Float(v) => s.push_str(&v.to_string()),
                                InfosValue::Flag => {
                                    s.push_str("true");
                                }
                                InfosValue::Integer(v) => s.push_str(&v.to_string()),
                                InfosValue::Array(arr) => match arr {
                                    InfosArray::Character(arr) => {
                                        let mut si = Vec::new();
                                        for v in arr.iter() {
                                            match v? {
                                                Some(v) => si.push(v.to_string()),
                                                None => {
                                                    si.push(String::from("."));
                                                }
                                            }
                                        }

                                        let new_s = si.join(",");
                                        s.push_str(&new_s);
                                    }
                                    InfosArray::Float(arr) => {
                                        let mut si = Vec::new();
                                        for v in arr.iter() {
                                            match v? {
                                                Some(v) => si.push(v.to_string()),
                                                None => si.push(String::from(".")),
                                            }
                                        }

                                        let new_s = si.join(",");
                                        s.push_str(&new_s);
                                    }
                                    InfosArray::Integer(arr) => {
                                        let mut si = Vec::new();
                                        for v in arr.iter() {
                                            match v? {
                                                Some(v) => si.push(v.to_string()),
                                                None => si.push(String::from('.')),
                                            }
                                        }

                                        let new_s = si.join(",");
                                        s.push_str(&new_s);
                                    }
                                    InfosArray::String(arr) => {
                                        let mut si = Vec::new();
                                        for v in arr.iter() {
                                            match v? {
                                                Some(v) => si.push(v.to_string()),
                                                None => si.push(String::from(".")),
                                            }
                                        }
                                        let new_s = si.join(",");
                                        s.push_str(&new_s);
                                    }
                                },
                            }

                            vs.push(s);
                        }

                        let s = vs.join(";");

                        builder.append_value(s);
                    }
                    InfosFormat::Struct(ref mut builder) => {
                        let info = record.info();

                        if info.is_empty() {
                            builder.append_null();
                            continue;
                        }

                        builder.append_value(info)?;
                    }
                },
                8 => match self.formats {
                    FormatsFormat::String(ref mut builder) => {
                        let samples = record.samples()?;

                        let mut column_names = Vec::new();
                        for name in samples.column_names(&self.header) {
                            let name = name?;
                            column_names.push(name);
                        }

                        let mut sample_strings = Vec::new();

                        for sample in samples.iter() {
                            let mut s = Vec::new();

                            for si in sample.iter(&self.header) {
                                let (_, value_option) = si?;
                                let value = value_option.unwrap();

                                match value {
                                    SamplesValue::String(v) => s.push(v.to_string()),
                                    SamplesValue::Character(v) => s.push(v.to_string()),
                                    SamplesValue::Float(v) => s.push(v.to_string()),
                                    SamplesValue::Genotype(gt) => {
                                        let mut gt_strs = Vec::new();
                                        let mut previous_phasing = "/";

                                        for gtt in gt.iter() {
                                            let (allele, phasing) = gtt?;

                                            let allele_str =
                                                allele.map_or(String::from("."), |v| v.to_string());

                                            // Determine the phasing symbol for the current allele based on the previous allele
                                            let phasing_str = match phasing {
                                                Phasing::Unphased => "/",
                                                Phasing::Phased => "|",
                                            };

                                            // For the first allele, don't prepend phasing symbol; otherwise, use the previous phasing
                                            if gt_strs.is_empty() {
                                                gt_strs.push(allele_str);
                                            } else {
                                                gt_strs.push(format!(
                                                    "{}{}",
                                                    previous_phasing, allele_str
                                                ));
                                            }

                                            // Update previous_phasing to the current allele's phasing for the next iteration
                                            previous_phasing = phasing_str;
                                        }

                                        s.push(gt_strs.join(""));
                                    }
                                    SamplesValue::Integer(v) => s.push(v.to_string()),
                                    SamplesValue::Array(arr) => match arr {
                                        Array::Character(ca) => {
                                            let mut si = Vec::new();
                                            for v in ca.iter() {
                                                if let Some(v) = v? {
                                                    si.push(v.to_string())
                                                }
                                            }

                                            let new_s = si.join(",");
                                            s.push(new_s);
                                        }
                                        Array::Float(arr) => {
                                            let mut si = Vec::new();
                                            for v in arr.iter() {
                                                match v? {
                                                    Some(v) => si.push(v.to_string()),
                                                    None => si.push(String::from(".")),
                                                }
                                            }

                                            let new_s = si.join(",");
                                            s.push(new_s);
                                        }
                                        Array::Integer(arr) => {
                                            let mut si = Vec::new();
                                            for v in arr.iter() {
                                                match v? {
                                                    Some(v) => si.push(v.to_string()),
                                                    None => si.push(String::from('.')),
                                                }
                                            }

                                            let new_s = si.join(",");
                                            s.push(new_s);
                                        }
                                        Array::String(arr) => {
                                            let mut si = Vec::new();
                                            for v in arr.iter() {
                                                match v? {
                                                    Some(v) => si.push(v.to_string()),
                                                    None => si.push(String::from('.')),
                                                }
                                            }

                                            let new_s = si.join(",");
                                            s.push(new_s);
                                        }
                                    },
                                }
                            }

                            sample_strings.push(s.join(":"));
                        }

                        let ss = column_names.join(":");
                        let ss = format!("{}\t{}", ss, sample_strings.join("\t"));

                        builder.append_value(ss);
                    }
                    FormatsFormat::List(ref mut builder) => {
                        let samples = record.samples()?;

                        if samples.is_empty() {
                            builder.append_null();
                            continue;
                        }

                        builder.append_value(samples, &self.header)?;
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
