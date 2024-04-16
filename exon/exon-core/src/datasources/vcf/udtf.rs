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
    error::ExonError,
};
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, function::TableFunctionImpl,
        listing::ListingTableUrl, TableProvider,
    },
    error::{DataFusionError, Result},
    execution::context::SessionContext,
    logical_expr::Expr,
    scalar::ScalarValue,
};
use exon_common::TableSchema;

use super::table_provider::{ListingVCFTable, ListingVCFTableOptions};

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

        let listing_table_config = ExonListingConfig::new_with_options(
            listing_scan_function.listing_table_url,
            listing_table_options,
        );

        let listing_table = ListingVCFTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}

/// A table function that returns a table provider for an Indexed VCF file.
pub struct VCFIndexedScanFunction {
    ctx: SessionContext,
}

impl VCFIndexedScanFunction {
    /// Create a new VCF scan function.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for VCFIndexedScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)))) = exprs.first() else {
            return Err(DataFusionError::Internal(
                "this function requires the path to be specified as the first argument".into(),
            ));
        };

        let listing_table_url = ListingTableUrl::parse(path)?;

        let Some(Expr::Literal(ScalarValue::Utf8(Some(region_str)))) = exprs.get(1) else {
            return Err(DataFusionError::Internal(
                "this function requires the region to be specified as the second argument".into(),
            ));
        };

        let region = region_str.parse().map_err(ExonError::from)?;

        let listing_table_options =
            ListingVCFTableOptions::new(FileCompressionType::GZIP, true).with_regions(vec![region]);

        let schema = futures::executor::block_on(async {
            let schema = listing_table_options
                .infer_schema(&self.ctx.state(), &listing_table_url)
                .await?;

            Ok::<TableSchema, datafusion::error::DataFusionError>(schema)
        })?;

        let listing_table_config =
            ExonListingConfig::new_with_options(listing_table_url, listing_table_options);

        let listing_table = ListingVCFTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}
