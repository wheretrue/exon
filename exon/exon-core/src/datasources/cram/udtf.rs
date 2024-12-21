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

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::TableProvider;
use datafusion::scalar::ScalarValue;
use datafusion::{
    datasource::function::TableFunctionImpl, execution::context::SessionContext, logical_expr::Expr,
};

use datafusion::error::Result as DataFusionResult;
use exon_common::TableSchema;

use crate::error::ExonError;

use super::table_provider::{ListingCRAMTable, ListingCRAMTableConfig};

/// A table function that provides a scan over a CRAM file.
pub struct CRAMScanFunction {
    ctx: SessionContext,
}

impl std::fmt::Debug for CRAMScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CRAMScanFunction").finish()
    }
}

impl CRAMScanFunction {
    /// Create a new `CRAMScanFunction`.
    pub fn _new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for CRAMScanFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        // Arguments are (cram_listing_location, fasta_repo)
        if exprs.len() != 2 {
            return Err(ExonError::ExecutionError(
                "CRAMScanFunction requires two arguments: cram_listing_location and fasta_repo"
                    .to_string(),
            )
            .into());
        }

        let Some(Expr::Literal(ScalarValue::Utf8(Some(ref cram_listing_location)))) = exprs.first()
        else {
            return Err(ExonError::ExecutionError(
                "CRAMScanFunction requires the cram_listing_location to be specified as the first argument".to_string(),
            ).into());
        };
        let listing_table_url = ListingTableUrl::parse(cram_listing_location)?;

        let Some(Expr::Literal(ScalarValue::Utf8(fasta_repo))) = exprs.get(1) else {
            return Err(ExonError::ExecutionError(
                "CRAMScanFunction requires the fasta_repo to be specified as the second argument"
                    .to_string(),
            )
            .into());
        };

        let listing_table_options = super::table_provider::ListingCRAMTableOptions::default()
            .with_fasta_reference(fasta_repo.clone());

        let schema = futures::executor::block_on(async {
            let schema = listing_table_options
                .infer_schema(&self.ctx.state(), &listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config =
            ListingCRAMTableConfig::new(listing_table_url, listing_table_options);

        let listing_table = ListingCRAMTable::try_new(listing_table_config, schema)?;

        Ok(Arc::new(listing_table))
    }
}
