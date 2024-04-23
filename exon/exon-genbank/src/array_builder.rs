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
        ArrayBuilder, ArrayRef, GenericListBuilder, GenericStringBuilder, StringBuilder,
        StructBuilder,
    },
    datatypes::{DataType, Field, Fields},
};
use gb_io::seq::Seq;

pub struct GenbankArrayBuilder {
    sequence: GenericStringBuilder<i32>,
    accession: GenericStringBuilder<i32>,
    comments: GenericListBuilder<i32, GenericStringBuilder<i32>>,
    contig: GenericStringBuilder<i32>,
    date: GenericStringBuilder<i32>,
    dblink: GenericStringBuilder<i32>,
    definition: GenericStringBuilder<i32>,
    division: GenericStringBuilder<i32>,
    keywords: GenericStringBuilder<i32>,
    molecule_type: GenericStringBuilder<i32>,
    name: GenericStringBuilder<i32>,
    source: GenericStringBuilder<i32>,
    version: GenericStringBuilder<i32>,
    topology: GenericStringBuilder<i32>,
    features: GenericListBuilder<i32, StructBuilder>,
}

impl GenbankArrayBuilder {
    pub fn new() -> Self {
        let qualifier_key_field = Field::new("keys", DataType::Utf8, false);
        let qualifier_value_field = Field::new("values", DataType::Utf8, true);
        let fields = Fields::from(vec![qualifier_key_field, qualifier_value_field]);

        let struct_builder = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(GenericStringBuilder::<i32>::new()),
                Box::new(GenericStringBuilder::<i32>::new()),
            ],
        );

        let qualifier_field = Field::new("item", DataType::Struct(fields), true);
        let qualifier_list_field =
            Field::new("item", DataType::List(Arc::new(qualifier_field)), true);

        let qualifiers_list_builder = GenericListBuilder::<i32, StructBuilder>::new(struct_builder);

        let kind_builder = GenericStringBuilder::<i32>::new();
        let location_builder = GenericStringBuilder::<i32>::new();

        let kind_field = Field::new("kind", DataType::Utf8, false);
        let location_field = Field::new("location", DataType::Utf8, false);

        let fields: Fields = Fields::from(vec![kind_field, location_field, qualifier_list_field]);

        let feature_builder: StructBuilder = StructBuilder::new(
            fields,
            vec![
                Box::new(kind_builder),
                Box::new(location_builder),
                Box::new(qualifiers_list_builder),
            ],
        );

        Self {
            sequence: GenericStringBuilder::new(),
            accession: GenericStringBuilder::new(),
            comments: GenericListBuilder::new(GenericStringBuilder::new()),
            contig: GenericStringBuilder::new(),
            date: GenericStringBuilder::new(),
            dblink: GenericStringBuilder::new(),
            definition: GenericStringBuilder::new(),
            division: GenericStringBuilder::new(),
            keywords: GenericStringBuilder::new(),
            molecule_type: GenericStringBuilder::new(),
            name: GenericStringBuilder::new(),
            source: GenericStringBuilder::new(),
            version: GenericStringBuilder::new(),
            topology: GenericStringBuilder::new(),
            features: GenericListBuilder::new(feature_builder),
        }
    }

    pub fn len(&self) -> usize {
        self.sequence.len()
    }

    pub fn finish(&mut self) -> Vec<ArrayRef> {
        let sequence = self.sequence.finish();
        let accession = self.accession.finish();
        let comments = self.comments.finish();
        let contig = self.contig.finish();
        let date = self.date.finish();
        let dblink = self.dblink.finish();
        let definition = self.definition.finish();
        let division = self.division.finish();
        let keywords = self.keywords.finish();
        let molecule_type = self.molecule_type.finish();
        let name = self.name.finish();
        let source = self.source.finish();
        let version = self.version.finish();
        let topology = self.topology.finish();
        let features = self.features.finish();

        vec![
            Arc::new(sequence),
            Arc::new(accession),
            Arc::new(comments),
            Arc::new(contig),
            Arc::new(date),
            Arc::new(dblink),
            Arc::new(definition),
            Arc::new(division),
            Arc::new(keywords),
            Arc::new(molecule_type),
            Arc::new(name),
            Arc::new(source),
            Arc::new(version),
            Arc::new(topology),
            Arc::new(features),
        ]
    }

    pub fn append(&mut self, record: &Seq) {
        let seq_str = std::str::from_utf8(&record.seq).unwrap();
        self.sequence.append_value(seq_str);

        self.accession.append_option(record.accession.as_ref());

        if !record.comments.is_empty() {
            let values = self.comments.values();

            record.comments.iter().for_each(|comment| {
                values.append_value(comment);
            });

            self.comments.append(true);
        } else {
            self.comments.append_null();
        }

        self.contig
            .append_option(record.contig.as_ref().map(|contig| contig.to_string()));
        self.date
            .append_option(record.date.as_ref().map(|date| date.to_string()));
        self.dblink.append_option(record.dblink.as_ref());
        self.definition.append_option(record.definition.as_ref());
        self.division.append_value(&record.division);
        self.keywords.append_option(record.keywords.as_ref());
        self.molecule_type
            .append_option(record.molecule_type.as_ref());

        self.name.append_option(record.name.as_ref());
        self.source.append_option(
            record
                .source
                .as_ref()
                .map(|source| source.source.to_string()),
        );

        self.version.append_option(record.version.as_ref());
        self.topology.append_value(record.topology.to_string());

        let seq_features = &record.features;

        let feature_values = self.features.values();
        for feature in seq_features {
            let kind = feature.kind.to_string();
            let location = feature.location.to_string();

            feature_values
                .field_builder::<StringBuilder>(0)
                .unwrap()
                .append_value(kind);

            feature_values
                .field_builder::<StringBuilder>(1)
                .unwrap()
                .append_value(location);

            let list_builder = feature_values
                .field_builder::<GenericListBuilder<i32, StructBuilder>>(2)
                .unwrap();

            for (k, v) in feature.qualifiers.iter() {
                list_builder
                    .values()
                    .field_builder::<GenericStringBuilder<i32>>(0)
                    .unwrap()
                    .append_value(k);

                list_builder
                    .values()
                    .field_builder::<GenericStringBuilder<i32>>(1)
                    .unwrap()
                    .append_option(v.as_ref());

                list_builder.values().append(true);
            }

            list_builder.append(true);

            feature_values.append(true);
        }
        self.features.append(true);
    }
}
