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

use crate::{
    datasources::{exon_listing_table_options::ExonListingConfig, ScanFunction},
    ExonRuntimeEnvExt,
};
use datafusion::{
    catalog::TableFunctionImpl, datasource::TableProvider, error::Result,
    execution::context::SessionContext, logical_expr::Expr,
};
use exon_gtf::new_gtf_schema_builder;

use super::table_provider::{ListingGTFTable, ListingGTFTableOptions};

/// A table function that returns a table provider for a GTF file.
pub struct GTFScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for GTFScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GTFScanFunction").finish()
    }
}

impl GTFScanFunction {
    /// Create a new `GTFScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for GTFScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_scan_function.listing_table_url.as_ref())
                .await
        })?;

        let schema = new_gtf_schema_builder().build();

        let listing_table_options =
            ListingGTFTableOptions::new(listing_scan_function.file_compression_type);

        let listing_table_config = ExonListingConfig::new_with_options(
            listing_scan_function.listing_table_url,
            listing_table_options,
        );

        let listing_table = ListingGTFTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}
