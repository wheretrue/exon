// Copyright 2024 WHERE TRUE Technologies.
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

use datafusion::{
    catalog::TableFunctionImpl, datasource::TableProvider, error::Result,
    execution::context::SessionContext, logical_expr::Expr,
};
use exon_fasta::FASTASchemaBuilder;

use crate::{
    datasources::{
        exon_listing_table_options::ExonListingConfig,
        fasta::table_provider::{ListingFASTATable, ListingFASTATableOptions},
        ScanFunction,
    },
    ExonRuntimeEnvExt,
};

/// A table function that returns a table provider for a FASTA file.
pub struct FastaScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for FastaScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastaScanFunction").finish()
    }
}

impl FastaScanFunction {
    /// Create a new `FastaScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for FastaScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_scan_function.listing_table_url.as_ref())
                .await
        })?;

        let fasta_schema = FASTASchemaBuilder::default().build();

        let listing_table_options =
            ListingFASTATableOptions::new(listing_scan_function.file_compression_type);

        let listing_table_config = ExonListingConfig::new_with_options(
            listing_scan_function.listing_table_url,
            listing_table_options,
        );

        let listing_table = ListingFASTATable::try_new(listing_table_config, fasta_schema)?;

        Ok(Arc::new(listing_table))
    }
}
