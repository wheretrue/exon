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

use std::{fmt::Debug, sync::Arc};

use crate::{
    config::extract_config_from_state,
    datasources::{exon_listing_table_options::ExonListingConfig, ScanFunction},
    error::ExonError,
    ExonRuntimeEnvExt,
};
use datafusion::{
    datasource::{function::TableFunctionImpl, listing::ListingTableUrl, TableProvider},
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    logical_expr::Expr,
    scalar::ScalarValue,
};
use exon_common::TableSchema;

use super::table_provider::{ListingBAMTable, ListingBAMTableOptions};

/// A table function that returns a table provider for a BAM file.
#[derive(Default)]
pub struct BAMScanFunction {
    ctx: SessionContext,
}

impl Debug for BAMScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BAMScanFunction").finish()
    }
}

impl BAMScanFunction {
    /// Create a new `BAMScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for BAMScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_scan_function.listing_table_url.as_ref())
                .await
        })?;

        let state = self.ctx.state();
        let config = extract_config_from_state(&state)?;

        let options = ListingBAMTableOptions::default().with_tag_as_struct(config.bam_parse_tags);

        let schema = futures::executor::block_on(async {
            let schema = options
                .infer_schema(&self.ctx.state(), &listing_scan_function.listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config =
            ExonListingConfig::new_with_options(listing_scan_function.listing_table_url, options);

        let listing_table = ListingBAMTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}

/// A table function that returns a table provider for an Indexed BAM file.
pub struct BAMIndexedScanFunction {
    ctx: SessionContext,
}

impl Debug for BAMIndexedScanFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BAMIndexedScanFunction").finish()
    }
}

impl BAMIndexedScanFunction {
    /// Create a new `BAMIndexedScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for BAMIndexedScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)))) = exprs.first() else {
            return Err(DataFusionError::Internal(
                "this function requires the path to be specified as the first argument".into(),
            ));
        };

        let listing_table_url = ListingTableUrl::parse(path)?;

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_table_url.as_ref())
                .await
        })?;

        let Some(Expr::Literal(ScalarValue::Utf8(Some(region_str)))) = exprs.get(1) else {
            return Err(DataFusionError::Internal(
                "this function requires the region to be specified as the second argument".into(),
            ));
        };

        let region = region_str.parse().map_err(ExonError::from)?;

        let state = self.ctx.state();
        let config = extract_config_from_state(&state)?;

        let options = ListingBAMTableOptions::default()
            .with_regions(vec![region])
            .with_tag_as_struct(config.bam_parse_tags);

        let schema = futures::executor::block_on(async {
            let schema = options
                .infer_schema(&self.ctx.state(), &listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config = ExonListingConfig::new_with_options(listing_table_url, options);

        let listing_table = ListingBAMTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}
