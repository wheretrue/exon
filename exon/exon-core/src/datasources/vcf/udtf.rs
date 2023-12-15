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
    execution::context::SessionContext,
    logical_expr::Expr,
};
use exon_common::TableSchema;

use super::table_provider::{ListingVCFTable, ListingVCFTableConfig, ListingVCFTableOptions};

/// A table function that returns a table provider for a VCF file.
pub struct VCFScanFunction {
    ctx: SessionContext,
}

impl VCFScanFunction {
    /// Create a new VCF scan function.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for VCFScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        let listing_table_options =
            ListingVCFTableOptions::new(listing_scan_function.file_compression_type, false);

        let schema = futures::executor::block_on(async {
            let schema = listing_table_options
                .infer_schema(&self.ctx.state(), &listing_scan_function.listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config =
            ListingVCFTableConfig::new(listing_scan_function.listing_table_url)
                .with_options(listing_table_options);

        let listing_table = ListingVCFTable::try_new(listing_table_config, schema)?;

        Ok(Arc::new(listing_table))
    }
}
