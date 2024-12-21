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

use super::table_provider::{ListingFCSTable, ListingFCSTableConfig, ListingFCSTableOptions};

/// A table function that returns a table provider for a FCS file.
pub struct FCSScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for FCSScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FCSScanFunction").finish()
    }
}

impl FCSScanFunction {
    /// Create a new FCS scan function.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for FCSScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        let listing_table_options =
            ListingFCSTableOptions::new(listing_scan_function.file_compression_type);

        let schema = futures::executor::block_on(async {
            let schema = listing_table_options
                .infer_schema(&self.ctx.state(), &listing_scan_function.listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config = ListingFCSTableConfig::new(
            listing_scan_function.listing_table_url,
            listing_table_options,
        );

        let listing_table = ListingFCSTable::try_new(listing_table_config, schema)?;

        Ok(Arc::new(listing_table))
    }
}
