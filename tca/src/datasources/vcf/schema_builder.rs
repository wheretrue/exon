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

use arrow::datatypes::{Field, Fields};
use noodles::vcf::{
    header::{
        record::value::map::format::Type as FormatType, record::value::map::info::Type as InfoType,
        Formats, Infos, Number as InfoNumber,
    },
    Header,
};

/// A builder for an arrow schema from a VCF header.
pub struct VCFSchemaBuilder {
    /// The fields of the schema.
    fields: Vec<arrow::datatypes::Field>,
}

impl VCFSchemaBuilder {
    /// Creates a new VCF schema builder.
    pub fn new() -> Self {
        Self {
            fields: vec![
                Field::new("chrom", arrow::datatypes::DataType::Utf8, false),
                Field::new("pos", arrow::datatypes::DataType::Int32, false),
                Field::new(
                    "id",
                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
                Field::new("ref", arrow::datatypes::DataType::Utf8, false),
                Field::new(
                    "alt",
                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
                Field::new("qual", arrow::datatypes::DataType::Float32, true),
                Field::new(
                    "filter",
                    arrow::datatypes::DataType::List(Arc::new(Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    false,
                ),
            ],
        }
    }

    /// Updates the schema from a VCF header.
    pub fn update_from_header(&mut self, header: &noodles::vcf::Header) {
        self.fields.push(vcf_info_to_field(header.infos().clone()));
        self.fields
            .push(vcf_formats_to_field(header.formats().clone()));
    }

    /// Builds the schema.
    pub fn build(self) -> arrow::datatypes::Schema {
        arrow::datatypes::Schema::new(self.fields)
    }
}

/// Creates a new builder from a VCF header.
impl From<Header> for VCFSchemaBuilder {
    fn from(header: Header) -> Self {
        let mut builder = Self::new();
        builder.update_from_header(&header);
        builder
    }
}

impl Default for VCFSchemaBuilder {
    fn default() -> Self {
        // Make all strings
        todo!("default")
    }
}

fn vcf_info_type_to_arrow_type(ty: InfoType) -> arrow::datatypes::DataType {
    match ty {
        InfoType::Integer => arrow::datatypes::DataType::Int32,
        InfoType::Float => arrow::datatypes::DataType::Float32,
        InfoType::Character => arrow::datatypes::DataType::Utf8,
        InfoType::String => arrow::datatypes::DataType::Utf8,
        InfoType::Flag => arrow::datatypes::DataType::Boolean,
    }
}

fn vcf_format_type_to_arrow_type(ty: FormatType) -> arrow::datatypes::DataType {
    match ty {
        FormatType::Character => arrow::datatypes::DataType::Utf8,
        FormatType::Integer => arrow::datatypes::DataType::Int32,
        FormatType::Float => arrow::datatypes::DataType::Float32,
        FormatType::String => arrow::datatypes::DataType::Utf8,
    }
}

fn wrap_type_in_count(cnt: InfoNumber, typ: &arrow::datatypes::Field) -> arrow::datatypes::Field {
    match cnt {
        InfoNumber::Count(0) => typ.clone(),
        InfoNumber::Count(1) => typ.clone(),
        _ => arrow::datatypes::Field::new(
            typ.name(),
            arrow::datatypes::DataType::List(Arc::new(typ.clone())),
            typ.is_nullable(),
        ),
    }
}

fn vcf_info_to_field(infos: Infos) -> arrow::datatypes::Field {
    let mut arrow_fields = Vec::new();

    for (key, value) in infos {
        let ty = vcf_info_type_to_arrow_type(value.ty());

        let field = arrow::datatypes::Field::new(key.to_string(), ty, false);
        let field = wrap_type_in_count(value.number(), &field);

        arrow_fields.push(field);
    }

    let fields = Fields::from(arrow_fields);

    arrow::datatypes::Field::new("info", arrow::datatypes::DataType::Struct(fields), false)
}

fn vcf_formats_to_field(formats: Formats) -> arrow::datatypes::Field {
    let mut fields = Vec::new();
    for (key, value) in formats {
        let ty = vcf_format_type_to_arrow_type(value.ty());

        let field = arrow::datatypes::Field::new(key.to_string(), ty, false);
        let field = wrap_type_in_count(value.number(), &field);

        fields.push(field);
    }

    let fields = Fields::from(fields);

    let field =
        arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Struct(fields), true);

    arrow::datatypes::Field::new(
        "formats",
        arrow::datatypes::DataType::List(Arc::new(field)),
        false,
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    #[test]
    fn test_default_header_to_schema() {
        let header = noodles::vcf::Header::default();
        let schema = super::VCFSchemaBuilder::from(header).build();

        assert_eq!(schema.fields().len(), 9);

        assert_eq!(schema.field(0).name(), "chrom");
        assert_eq!(
            schema.field(0).data_type(),
            &arrow::datatypes::DataType::Utf8
        );

        assert_eq!(schema.field(1).name(), "pos");
        assert_eq!(
            schema.field(1).data_type(),
            &arrow::datatypes::DataType::Int32
        );

        assert_eq!(schema.field(2).name(), "id");
        assert_eq!(
            schema.field(2).data_type(),
            &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::Utf8,
                true
            )))
        );

        assert_eq!(schema.field(3).name(), "ref");
        assert_eq!(
            schema.field(3).data_type(),
            &arrow::datatypes::DataType::Utf8
        );

        assert_eq!(schema.field(4).name(), "alt");
        assert_eq!(
            schema.field(4).data_type(),
            &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::Utf8,
                true
            )))
        );

        assert_eq!(schema.field(5).name(), "qual");
        assert_eq!(
            schema.field(5).data_type(),
            &arrow::datatypes::DataType::Float32
        );

        assert_eq!(schema.field(6).name(), "filter");
        assert_eq!(
            schema.field(6).data_type(),
            &arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                arrow::datatypes::DataType::Utf8,
                true
            )))
        );

        // TODO: better testing around info and formats
        assert_eq!(schema.field(7).name(), "info");
        assert_eq!(schema.field(8).name(), "formats");
    }
}
