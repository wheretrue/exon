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

mod genotype_builder;
mod info_builder;

use std::sync::Arc;

use arrow::{
    array::{
        ArrayBuilder, ArrayRef, Float32Builder, GenericListBuilder, GenericStringBuilder,
        Int32Builder,
    },
    datatypes::SchemaRef,
    error::ArrowError,
};
use noodles::vcf::Record;

use self::genotype_builder::GenotypeBuilder;
use self::info_builder::InfosBuilder;

/// A builder for creating a `ArrayRef` from a `VCF` file.
pub struct VCFArrayBuilder {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int32Builder,
    ids: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    references: GenericStringBuilder<i32>,
    alternates: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    qualities: Float32Builder,
    filters: GenericListBuilder<i32, GenericStringBuilder<i32>>,

    // TODO: VCF IMPV: maybe string builder for info and format, or maybe not existing
    // May not need to change though, if the field in `create` can be used to create the builder
    // (i.e. maybe just String)
    infos: InfosBuilder,
    formats: GenotypeBuilder,

    projection: Vec<usize>,
}

impl VCFArrayBuilder {
    /// Creates a new `VCFArrayBuilder` from a `Schema`.
    pub fn create(
        schema: SchemaRef,
        capacity: usize,
        projection: Option<Vec<usize>>,
    ) -> Result<Self, ArrowError> {
        let info_field = schema.field_with_name("info")?;
        let format_field = schema.field_with_name("formats")?;

        let projection = match projection {
            Some(projection) => projection,
            None => (0..schema.fields().len()).collect(),
        };

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

            projection,
        })
    }

    /// Returns the number of records in the builder.
    pub fn len(&self) -> usize {
        self.chromosomes.len()
    }

    /// Returns whether the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Appends a record to the builder.
    pub fn append(&mut self, record: &noodles::vcf::lazy::Record) {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let chromosome: String = format!("{}", record.chromosome());
                    self.chromosomes.append_value(chromosome);
                }
                1 => {
                    todo!();
                    // let position: usize = record.position().into();
                    // self.positions.append_value(position as i32);
                }
                2 => {
                    todo!();
                    // for id in record.ids().iter() {
                    //     self.ids.values().append_value(id.to_string());
                    // }

                    // self.ids.append(true);
                }
                3 => {
                    todo!();
                    // let reference: String = format!("{}", record.reference_bases());
                    // self.references.append_value(reference);
                }
                4 => {
                    todo!();
                    // for alt in record.alternate_bases().iter() {
                    //     self.alternates.values().append_value(alt.to_string());
                    // }

                    // self.alternates.append(true);
                }
                5 => {
                    todo!();
                    // let quality = record.quality_score().map(f32::from);
                    // self.qualities.append_option(quality);
                }
                6 => {
                    todo!();
                    // for filter in record.filters().iter() {
                    //     self.filters.values().append_value(filter.to_string());
                    // }
                    // self.filters.append(true);
                }
                // 7 => self.infos.append_value(record.info()),
                // 8 => self.formats.append_value(record.genotypes()),
                _ => panic!("Not implemented"),
            }
        }
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
                7 => arrays.push(Arc::new(self.infos.finish())),
                8 => arrays.push(Arc::new(self.formats.finish())),
                _ => panic!("Not implemented"),
            }
        }

        arrays
    }
}
