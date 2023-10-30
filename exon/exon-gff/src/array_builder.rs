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
    array::{
        ArrayBuilder, ArrayRef, Float32Builder, GenericListBuilder, GenericStringBuilder,
        Int64Builder, MapBuilder,
    },
    error::ArrowError,
};
use noodles::gff::{record::attributes::field::Value, Record};

pub struct GFFArrayBuilder {
    seqnames: GenericStringBuilder<i32>,
    sources: GenericStringBuilder<i32>,
    feature_types: GenericStringBuilder<i32>,
    starts: Int64Builder,
    ends: Int64Builder,
    scores: Float32Builder,
    strands: GenericStringBuilder<i32>,
    phases: GenericStringBuilder<i32>,
    attributes:
        MapBuilder<GenericStringBuilder<i32>, GenericListBuilder<i32, GenericStringBuilder<i32>>>,
}

/// A default implementation for GFFArrayBuilder.
impl Default for GFFArrayBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GFFArrayBuilder {
    pub fn new() -> Self {
        Self {
            seqnames: GenericStringBuilder::<i32>::new(),
            sources: GenericStringBuilder::<i32>::new(),
            feature_types: GenericStringBuilder::<i32>::new(),
            starts: Int64Builder::new(),
            ends: Int64Builder::new(),
            scores: Float32Builder::new(),
            strands: GenericStringBuilder::<i32>::new(),
            phases: GenericStringBuilder::<i32>::new(),
            attributes: MapBuilder::new(
                None,
                GenericStringBuilder::<i32>::new(),
                GenericListBuilder::<i32, GenericStringBuilder<i32>>::new(GenericStringBuilder::<
                    i32,
                >::new()),
            ),
        }
    }

    /// Returns the number of records in the array builder.
    pub fn len(&self) -> usize {
        self.seqnames.len()
    }

    /// Returns whether the array builder is empty.
    pub fn is_empty(&self) -> bool {
        self.seqnames.is_empty()
    }

    pub fn append(&mut self, record: &Record) -> Result<(), ArrowError> {
        self.seqnames.append_value(record.reference_sequence_name());
        self.sources.append_value(record.source());
        self.feature_types.append_value(record.ty());
        self.starts.append_value(record.start().get() as i64);
        self.ends.append_value(record.end().get() as i64);
        self.scores.append_option(record.score());
        self.strands.append_value(record.strand());
        self.phases
            .append_option(record.phase().map(|p| p.to_string()));

        for (key, value) in record.attributes().iter() {
            self.attributes.keys().append_value(key);

            match value {
                Value::String(value) => {
                    let values = self.attributes.values();
                    let list_values = values.values();

                    list_values.append_value(value.as_str());
                    values.append(true);
                }
                Value::Array(attr_values) => {
                    let values = self.attributes.values();
                    let list_values = values.values();

                    for value in attr_values.iter() {
                        list_values.append_value(value.as_str());
                    }

                    values.append(true);
                }
            }

            // self.attributes.values().append_option(value.as_string());
        }

        self.attributes.append(true)?;

        Ok(())
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let seqnames = self.seqnames.finish();
        let sources = self.sources.finish();
        let feature_types = self.feature_types.finish();
        let starts = self.starts.finish();
        let ends = self.ends.finish();
        let scores = self.scores.finish();
        let strands = self.strands.finish();
        let phases = self.phases.finish();
        let attributes = self.attributes.finish();

        vec![
            Arc::new(seqnames),
            Arc::new(sources),
            Arc::new(feature_types),
            Arc::new(starts),
            Arc::new(ends),
            Arc::new(scores),
            Arc::new(strands),
            Arc::new(phases),
            Arc::new(attributes),
        ]
    }
}
