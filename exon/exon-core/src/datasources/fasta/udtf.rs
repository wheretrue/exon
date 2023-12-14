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

use datafusion::{
    common::plan_err,
    datasource::{
        file_format::file_compression_type::FileCompressionType, function::TableFunctionImpl,
        listing::ListingTableUrl, TableProvider,
    },
    error::{DataFusionError, Result},
    logical_expr::Expr,
    scalar::ScalarValue,
};
use exon_fasta::new_fasta_schema_builder;

use super::table_provider::{ListingFASTATable, ListingFASTATableConfig, ListingFASTATableOptions};

/// A table function that returns a table provider for a FASTA file.
#[derive(Debug, Default)]
pub struct FastaScanFunction {}

impl TableFunctionImpl for FastaScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(ref path)))) = exprs.first() else {
            return plan_err!("fasta_scan requires at least one string argument");
        };

        let passed_compression_type = exprs.get(1).and_then(|e| match e {
            Expr::Literal(ScalarValue::Utf8(Some(ref compression_type))) => {
                FileCompressionType::from_str(compression_type).ok()
            }
            _ => None,
        });

        let listing_table_url = ListingTableUrl::parse(path)?;
        let inferred_compression = listing_table_url
            .prefix()
            .extension()
            .and_then(|ext| FileCompressionType::from_str(ext).ok());

        let compression_type = passed_compression_type
            .or(inferred_compression)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let fasta_schema = new_fasta_schema_builder().build();

        let listing_table_options = ListingFASTATableOptions::new(compression_type);
        let listing_fasta_table_config =
            ListingFASTATableConfig::new(listing_table_url).with_options(listing_table_options);

        let listing_fasta_table =
            ListingFASTATable::try_new(listing_fasta_table_config, fasta_schema)?;

        Ok(Arc::new(listing_fasta_table))
    }
}
