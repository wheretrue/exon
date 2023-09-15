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
use datafusion::error::Result;
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

    /// The header to use for schema inference.
    header: Option<Header>,

    /// Whether to parse the INFO field.
    parse_info: bool,

    /// Whether to parse the FORMAT field.
    parse_formats: bool,
}

impl VCFSchemaBuilder {
    /// Add a header to the schema builder.
    pub fn with_header(mut self, header: Header) -> Self {
        self.header = Some(header);
        self
    }

    /// Set the parse_info flag.
    pub fn with_parse_info(mut self, parse_info: bool) -> Self {
        self.parse_info = parse_info;
        self
    }

    /// Set the parse_formats flag.
    pub fn with_parse_formats(mut self, parse_formats: bool) -> Self {
        self.parse_formats = parse_formats;
        self
    }
}

impl Default for VCFSchemaBuilder {
    fn default() -> Self {
        Self {
            fields: vec![
                Field::new("chrom", arrow::datatypes::DataType::Utf8, false),
                Field::new("pos", arrow::datatypes::DataType::Int64, false),
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
                    true,
                ),
                Field::new("info", arrow::datatypes::DataType::Utf8, true),
                Field::new("formats", arrow::datatypes::DataType::Utf8, true),
            ],
            parse_info: false,
            parse_formats: false,
            header: None,
        }
    }
}

impl VCFSchemaBuilder {
    /// Updates the schema from a VCF header.
    pub fn update_from_header(&mut self, header: &noodles::vcf::Header) {
        self.fields.push(vcf_info_to_field(header.infos().clone()));
        self.fields
            .push(vcf_formats_to_field(header.formats().clone()));
    }

    /// Builds the schema.
    pub fn build(&mut self) -> Result<arrow::datatypes::Schema> {
        // If both parse_info and parse_formats are false, then we can just return the default schema
        if !self.parse_info && !self.parse_formats {
            return Ok(arrow::datatypes::Schema::new(self.fields.clone()));
        }

        // Make sure we have a header to parse
        let header = match &self.header {
            Some(header) => header,
            None => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "No header provided to parse".to_string(),
                ))
            }
        };

        // If we are parsing info, then we need to update the info field
        if self.parse_info {
            let info_field = vcf_info_to_field(header.infos().clone());
            self.fields[7] = info_field;
        }

        if self.parse_formats {
            let format_field = vcf_formats_to_field(header.formats().clone());
            self.fields[8] = format_field;
        }

        Ok(arrow::datatypes::Schema::new(self.fields.clone()))
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
            arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                typ.data_type().clone(),
                typ.is_nullable(),
            ))),
            typ.is_nullable(),
        ),
    }
}

fn vcf_info_to_field(infos: Infos) -> arrow::datatypes::Field {
    let mut arrow_fields = Vec::new();

    for (key, value) in infos {
        let ty = vcf_info_type_to_arrow_type(value.ty());

        let field = arrow::datatypes::Field::new(key.to_string(), ty, true);
        let field = wrap_type_in_count(value.number(), &field);

        arrow_fields.push(field);
    }

    let fields = Fields::from(arrow_fields);

    arrow::datatypes::Field::new("info", arrow::datatypes::DataType::Struct(fields), true)
}

fn vcf_formats_to_field(formats: Formats) -> arrow::datatypes::Field {
    let mut fields = Vec::new();
    for (key, value) in formats {
        let ty = vcf_format_type_to_arrow_type(value.ty());

        let field = arrow::datatypes::Field::new(key.to_string(), ty, true);
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
    use std::{str::FromStr, sync::Arc};

    use noodles::vcf::{
        header::{
            record::value::{
                map::{format, info},
                Map,
            },
            Number,
        },
        record::genotypes,
        record::info::field::Key,
        Header,
    };

    use super::VCFSchemaBuilder;

    #[test]
    fn test_genotype_schema_inference() -> Result<(), Box<dyn std::error::Error>> {
        let mut header_builder = Header::builder();
        let mut expected_fields = Vec::new();

        let test_table = vec![
            (
                "single_int",
                Number::Count(1),
                format::Type::Integer,
                arrow::datatypes::Field::new("single_int", arrow::datatypes::DataType::Int32, true),
            ),
            (
                "single_float",
                Number::Count(1),
                format::Type::Float,
                arrow::datatypes::Field::new(
                    "single_float",
                    arrow::datatypes::DataType::Float32,
                    true,
                ),
            ),
            (
                "single_char",
                Number::Count(1),
                format::Type::Character,
                arrow::datatypes::Field::new("single_char", arrow::datatypes::DataType::Utf8, true),
            ),
            (
                "single_string",
                Number::Count(1),
                format::Type::String,
                arrow::datatypes::Field::new(
                    "single_string",
                    arrow::datatypes::DataType::Utf8,
                    true,
                ),
            ),
            (
                "single_int_array",
                Number::Count(2),
                format::Type::Integer,
                arrow::datatypes::Field::new(
                    "single_int_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Int32,
                        true,
                    ))),
                    true,
                ),
            ),
            (
                "single_float_array",
                Number::Count(2),
                format::Type::Float,
                arrow::datatypes::Field::new(
                    "single_float_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Float32,
                        true,
                    ))),
                    true,
                ),
            ),
            (
                "single_char_array",
                Number::Count(2),
                format::Type::Character,
                arrow::datatypes::Field::new(
                    "single_char_array",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
            ),
        ];

        for (a, b, c, d) in test_table {
            let key = genotypes::keys::Key::from_str(a).unwrap();
            let format = Map::builder()
                .set_description("test")
                .set_number(b)
                .set_type(c)
                .set_idx(1)
                .build()
                .unwrap();

            header_builder = header_builder.add_format(key, format);

            expected_fields.push(d);
        }

        let header = header_builder.build();

        let schema = VCFSchemaBuilder::default()
            .with_header(header)
            .with_parse_formats(true)
            .build()?;

        let info_field = schema.field(8);

        let inner_struct =
            &arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(expected_fields));
        let inner_field = arrow::datatypes::Field::new("item", inner_struct.clone(), true);
        let expected_type = arrow::datatypes::DataType::List(Arc::new(inner_field));

        assert_eq!(info_field.name(), "formats");
        assert_eq!(info_field.data_type(), &expected_type);

        Ok(())
    }

    #[test]
    fn test_info_schema_inference() {
        let info_test_table = vec![
            (
                "single_int",
                Number::Count(1),
                info::Type::Integer,
                arrow::datatypes::Field::new("single_int", arrow::datatypes::DataType::Int32, true),
            ),
            (
                "single_str",
                Number::Count(1),
                info::Type::String,
                arrow::datatypes::Field::new("single_str", arrow::datatypes::DataType::Utf8, true),
            ),
            (
                "single_flag",
                Number::Count(0),
                info::Type::Flag,
                arrow::datatypes::Field::new(
                    "single_flag",
                    arrow::datatypes::DataType::Boolean,
                    true,
                ),
            ),
            (
                "single_char",
                Number::Count(1),
                info::Type::Character,
                arrow::datatypes::Field::new("single_char", arrow::datatypes::DataType::Utf8, true),
            ),
            (
                "single_float",
                Number::Count(1),
                info::Type::Float,
                arrow::datatypes::Field::new(
                    "single_float",
                    arrow::datatypes::DataType::Float32,
                    true,
                ),
            ),
            (
                "array_int",
                Number::Count(2),
                info::Type::Integer,
                arrow::datatypes::Field::new(
                    "array_int",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Int32,
                        true,
                    ))),
                    true,
                ),
            ),
            (
                "array_str",
                Number::Count(2),
                info::Type::String,
                arrow::datatypes::Field::new(
                    "array_str",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
            ),
            (
                "array_char",
                Number::Count(2),
                info::Type::Character,
                arrow::datatypes::Field::new(
                    "array_char",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Utf8,
                        true,
                    ))),
                    true,
                ),
            ),
            (
                "array_flag",
                Number::Count(2),
                info::Type::Flag,
                arrow::datatypes::Field::new(
                    "array_flag",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Boolean,
                        true,
                    ))),
                    true,
                ),
            ),
            (
                "array_float",
                Number::Count(2),
                info::Type::Float,
                arrow::datatypes::Field::new(
                    "array_float",
                    arrow::datatypes::DataType::List(Arc::new(arrow::datatypes::Field::new(
                        "item",
                        arrow::datatypes::DataType::Float32,
                        true,
                    ))),
                    true,
                ),
            ),
        ];

        let mut fields = Vec::new();
        let mut header = Header::builder();

        for (key_str, number, ty, field) in info_test_table {
            let key = Key::from_str(key_str).unwrap();
            let info = Map::builder()
                .set_description(key_str)
                .set_number(number)
                .set_type(ty)
                .build()
                .unwrap();

            header = header.add_info(key, info.clone());
            fields.push(field);
        }

        let header = header.build();
        let schema = VCFSchemaBuilder::default()
            .with_header(header)
            .with_parse_info(true)
            .build()
            .unwrap();

        let info_field = schema.field(7);

        assert_eq!(info_field.name(), "info");
        assert_eq!(
            info_field.data_type(),
            &arrow::datatypes::DataType::Struct(arrow::datatypes::Fields::from(fields))
        );
    }

    #[test]
    fn test_default_header_to_schema() {
        let schema = super::VCFSchemaBuilder::default().build().unwrap();

        assert_eq!(schema.fields().len(), 9);

        assert_eq!(schema.field(0).name(), "chrom");
        assert_eq!(
            schema.field(0).data_type(),
            &arrow::datatypes::DataType::Utf8
        );

        assert_eq!(schema.field(1).name(), "pos");
        assert_eq!(
            schema.field(1).data_type(),
            &arrow::datatypes::DataType::Int64
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

        assert_eq!(schema.field(7).name(), "info");
        assert_eq!(
            schema.field(7).data_type(),
            &arrow::datatypes::DataType::Utf8,
        );

        assert_eq!(schema.field(8).name(), "formats");
        assert_eq!(
            schema.field(8).data_type(),
            &arrow::datatypes::DataType::Utf8,
        );
    }
}
