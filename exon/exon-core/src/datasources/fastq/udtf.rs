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
    datasource::{function::TableFunctionImpl, TableProvider},
    error::Result,
    execution::context::SessionContext,
    logical_expr::Expr,
};
use exon_fastq::new_fastq_schema_builder;

use crate::{
    datasources::{exon_listing_table_options::ExonListingConfig, ScanFunction},
    ExonRuntimeEnvExt,
};

use super::table_provider::{ListingFASTQTable, ListingFASTQTableOptions};

/// A table function that returns a table provider for a FASTQ file.
pub struct FastqScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for FastqScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FastqScanFunction").finish()
    }
}

impl FastqScanFunction {
    /// Create a new `FastqScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for FastqScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_scan_function.listing_table_url.as_ref())
                .await
        })?;

        let fasta_schema = new_fastq_schema_builder().build();

        let options = ListingFASTQTableOptions::new(listing_scan_function.file_compression_type);

        let listing_table_config =
            ExonListingConfig::new_with_options(listing_scan_function.listing_table_url, options);

        let listing_table = ListingFASTQTable::try_new(listing_table_config, fasta_schema)?;

        Ok(Arc::new(listing_table))
    }
}
