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
    datatypes::SchemaRef,
    error::ArrowError,
};
use exon_common::ExonArrayBuilder;
use noodles::vcf::{
    variant::record::{AlternateBases, Filters, Ids},
    Header,
};

use noodles::vcf::variant::Record as VCFRecord;

use super::{GenotypeBuilder, InfosBuilder};

/// A builder for creating a `ArrayRef` from a `VCF` file.
pub struct VCFArrayBuilder {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int64Builder,
    ids: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    references: GenericStringBuilder<i32>,
    alternates: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    qualities: Float32Builder,
    filters: GenericListBuilder<i32, GenericStringBuilder<i32>>,

    infos: InfosBuilder,
    formats: GenotypeBuilder,

    header: Arc<Header>,

    projection: Vec<usize>,

    n_rows: usize,
}

impl VCFArrayBuilder {
    /// Creates a new `VCFArrayBuilder` from a `Schema`.
    pub fn create(
        schema: SchemaRef,
        capacity: usize,
        projection: Option<Vec<usize>>,
        header: Arc<Header>,
    ) -> Result<Self, ArrowError> {
        let info_field = schema.field_with_name("info")?;
        let format_field = schema.field_with_name("formats")?;

        let projection = match projection {
            Some(projection) => projection.to_vec(),
            None => (0..schema.fields().len()).collect(),
        };

        Ok(Self {
            n_rows: 0,
            chromosomes: GenericStringBuilder::<i32>::new(),
            positions: Int64Builder::new(),
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

            infos: InfosBuilder::try_new(info_field, header.clone(), capacity)?,

            formats: GenotypeBuilder::try_new(format_field, capacity)?,
            header,

            projection,
        })
    }

    /// Appends a record to the builder.
    pub fn append<T>(&mut self, record: T) -> Result<(), ArrowError>
    where
        T: VCFRecord,
    {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let chromosome = record.reference_sequence_name(&self.header)?.to_string();
                    self.chromosomes.append_value(chromosome);
                }
                1 => {
                    if let Some(position) = record.variant_start() {
                        let position = position?;
                        self.positions.append_value(position.get() as i64);
                    } else {
                        self.positions.append_null();
                    }
                }
                2 => {
                    for id in record.ids().iter() {
                        self.ids.values().append_value(id);
                    }

                    self.ids.append(true);
                }
                3 => {
                    let mut s = String::new();
                    for base in record.reference_bases().iter() {
                        let base = base?.into();
                        s.push(base);
                    }
                    self.references.append_value(s);
                }
                4 => {
                    for alt in record.alternate_bases().iter() {
                        let alt = alt?;
                        self.alternates.values().append_value(alt);
                    }

                    self.alternates.append(true);
                }
                5 => {
                    let quality_score = record.quality_score().transpose()?;
                    self.qualities.append_option(quality_score);
                }
                6 => {
                    let filters = record.filters();

                    for filter in filters.iter(&self.header) {
                        let filter = filter?;
                        self.filters.values().append_value(filter);
                    }

                    self.filters.append(true);
                }
                7 => {
                    let info = record.info();
                    self.infos.append_value(info)?;
                }
                8 => {
                    let samples = record.samples()?;
                    self.formats.append_value(samples, &self.header)?;
                }
                _ => Err(ArrowError::InvalidArgumentError(
                    "Invalid column index".to_string(),
                ))?,
            }
        }

        self.n_rows += 1;

        Ok(())
    }
}

impl ExonArrayBuilder for VCFArrayBuilder {
    fn finish(&mut self) -> Vec<ArrayRef> {
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
                7 => arrays.push(Arc::new(self.infos.finish())),
                8 => arrays.push(Arc::new(self.formats.finish())),
                _ => panic!("Not implemented"),
            }
        }

        arrays
    }

    fn len(&self) -> usize {
        self.n_rows
    }
}
