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

use crate::datasources::{exon_listing_table_options::ExonListingConfig, ScanFunction};
use datafusion::{
    catalog::TableFunctionImpl, datasource::TableProvider, error::Result,
    execution::context::SessionContext, logical_expr::Expr,
};
use exon_common::TableSchema;

use super::table_provider::{ListingBCFTable, ListingBCFTableOptions};

/// A table function that returns a table provider for a BCF file.
#[derive(Default)]
pub struct BCFScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for BCFScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BCFScanFunction").finish()
    }
}

impl BCFScanFunction {
    /// Create a new BCF scan function.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for BCFScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        let options = ListingBCFTableOptions::default();

        let schema = futures::executor::block_on(async {
            let schema = options
                .infer_schema(&self.ctx.state(), &listing_scan_function.listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let config =
            ExonListingConfig::new_with_options(listing_scan_function.listing_table_url, options);

        let listing_table = ListingBCFTable::new(config, schema);

        Ok(Arc::new(listing_table))
    }
}
