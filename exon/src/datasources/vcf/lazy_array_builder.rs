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
    array::{
        ArrayBuilder, ArrayRef, Float32Builder, GenericListBuilder, GenericStringBuilder,
        Int64Builder,
    },
    datatypes::SchemaRef,
    error::ArrowError,
};
use noodles::vcf::{
    record::{AlternateBases, Chromosome, Filters, Ids, Position, QualityScore, ReferenceBases},
    Header,
};

/// A builder for creating a `ArrayRef` from a `VCF` file.
pub struct LazyVCFArrayBuilder {
    chromosomes: GenericStringBuilder<i32>,
    positions: Int64Builder,
    ids: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    references: GenericStringBuilder<i32>,
    alternates: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    qualities: Float32Builder,
    filters: GenericListBuilder<i32, GenericStringBuilder<i32>>,

    // TODO: VCF IMPV: maybe string builder for info and format, or maybe not existing
    // May not need to change though, if the field in `create` can be used to create the builder
    // (i.e. maybe just String)
    // infos: InfosBuilder,
    // formats: GenotypeBuilder,
    projection: Vec<usize>,

    // Allow dead code
    #[allow(dead_code)]
    header: Arc<Header>,
}

impl LazyVCFArrayBuilder {
    /// Creates a new `VCFArrayBuilder` from a `Schema`.
    pub fn create(
        schema: SchemaRef,
        _capacity: usize,
        projection: Option<Vec<usize>>,
        header: Arc<Header>,
    ) -> Result<Self, ArrowError> {
        // let info_field = schema.field_with_name("info")?;
        // let format_field = schema.field_with_name("formats")?;

        let projection = match projection {
            Some(projection) => projection,
            None => (0..schema.fields().len()).collect(),
        };

        Ok(Self {
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

            // infos: InfosBuilder::try_new(info_field, capacity)?,
            // formats: GenotypeBuilder::try_new(format_field, capacity)?,
            projection,

            header,
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
    pub fn append(&mut self, record: &noodles::vcf::lazy::Record) -> Result<(), ArrowError> {
        for col_idx in self.projection.iter() {
            match col_idx {
                0 => {
                    let chromosome = Chromosome::from_str(record.chromosome())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    self.chromosomes.append_value(chromosome.to_string());
                }
                1 => {
                    let position = Position::from_str(record.position()).unwrap();

                    let pos_usize: usize = position.into();

                    self.positions.append_value(pos_usize as i64);
                }
                2 => {
                    let ids = Ids::from_str(record.ids())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    for id in ids.iter() {
                        self.ids.values().append_value(id.to_string());
                    }

                    self.ids.append(true);
                }
                3 => {
                    let reference_bases = ReferenceBases::from_str(record.reference_bases())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;
                    self.references.append_value(reference_bases.to_string());
                }
                4 => {
                    let alternate_bases = AlternateBases::from_str(record.alternate_bases())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    for alt in alternate_bases.iter() {
                        self.alternates.values().append_value(alt.to_string());
                    }

                    self.alternates.append(true);
                }
                5 => match QualityScore::from_str(record.quality_score()) {
                    Ok(quality_score) => self.qualities.append_value(f32::from(quality_score)),
                    Err(_) => {
                        self.qualities.append_null();
                    }
                },
                6 => {
                    let filters = Filters::from_str(record.filters())
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))?;

                    match filters {
                        Filters::Pass => {
                            self.filters.values().append_value("PASS");
                        }
                        Filters::Fail(ids) => {
                            for id in ids.iter() {
                                self.filters.values().append_value(id);
                            }
                        }
                    }

                    self.filters.append(true);
                }
                _ => panic!("Not implemented"),
            }
        }
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
                _ => panic!("Not implemented"),
            }
        }

        arrays
    }
}
