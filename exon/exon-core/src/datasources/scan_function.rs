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

use std::str::FromStr;

use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, listing::ListingTableUrl,
    },
    error::{DataFusionError, Result},
    logical_expr::Expr,
    scalar::ScalarValue,
};

pub(crate) struct ScanFunction {
    pub listing_table_url: ListingTableUrl,
    pub file_compression_type: FileCompressionType,
}

impl TryFrom<&[Expr]> for ScanFunction {
    type Error = DataFusionError;

    fn try_from(exprs: &[Expr]) -> Result<Self> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(ref path)))) = exprs.first() else {
            return Err(DataFusionError::Internal(
                "listing_scan requires at least one string argument".to_string(),
            ));
        };

        let listing_table_url = ListingTableUrl::parse(path)?;

        let passed_compression_type = exprs.get(1).and_then(|e| match e {
            Expr::Literal(ScalarValue::Utf8(Some(ref compression_type))) => {
                FileCompressionType::from_str(compression_type).ok()
            }
            _ => None,
        });

        let inferred_compression = listing_table_url
            .prefix()
            .extension()
            .and_then(|ext| FileCompressionType::from_str(ext).ok());

        let file_compression_type = passed_compression_type.or(inferred_compression);

        Ok(Self {
            listing_table_url,
            file_compression_type: file_compression_type
                .unwrap_or(FileCompressionType::UNCOMPRESSED),
        })
    }
}
