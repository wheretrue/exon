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

use super::table_provider::{ListingSAMTable, ListingSAMTableOptions};
use crate::{
    config::extract_config_from_state,
    datasources::{exon_listing_table_options::ExonListingConfig, ScanFunction},
};
use datafusion::{
    datasource::{function::TableFunctionImpl, TableProvider},
    error::Result,
    execution::context::SessionContext,
    logical_expr::Expr,
};
use exon_common::TableSchema;

/// A table function that returns a table provider for a SAM file.
pub struct SAMScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for SAMScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SAMScanFunction").finish()
    }
}

impl SAMScanFunction {
    /// Create a new `SAMScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for SAMScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        let state = self.ctx.state();
        let config = extract_config_from_state(&state)?;

        let listing_table_options =
            ListingSAMTableOptions::default().with_tag_as_struct(config.sam_parse_tags);

        let schema = futures::executor::block_on(async {
            let schema = listing_table_options
                .infer_schema(&state, &listing_scan_function.listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config = ExonListingConfig::new_with_options(
            listing_scan_function.listing_table_url,
            listing_table_options,
        );

        let listing_table = ListingSAMTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}
