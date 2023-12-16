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

use crate::datasources::ScanFunction;
use datafusion::{
    datasource::{function::TableFunctionImpl, TableProvider},
    error::Result,
    logical_expr::Expr,
};
use exon_gtf::new_gtf_schema_builder;

use super::table_provider::{ListingGTFTable, ListingGTFTableConfig, ListingGTFTableOptions};

/// A table function that returns a table provider for a GTF file.
#[derive(Debug, Default)]
pub struct GTFScanFunction {}

impl TableFunctionImpl for GTFScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        let schema = new_gtf_schema_builder().build();

        let listing_table_options =
            ListingGTFTableOptions::new(listing_scan_function.file_compression_type);

        let listing_table_config =
            ListingGTFTableConfig::new(listing_scan_function.listing_table_url)
                .with_options(listing_table_options);

        let listing_table = ListingGTFTable::try_new(listing_table_config, schema)?;

        Ok(Arc::new(listing_table))
    }
}
