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
    ExonRuntimeEnvExt,
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
use exon_gff::new_gff_schema_builder;

use super::table_provider::{ListingGFFTable, ListingGFFTableOptions};

/// A table function that returns a table provider for a GFF file.
pub struct GFFScanFunction {
    ctx: SessionContext,
}

impl GFFScanFunction {
    /// Create a new `GFFScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for GFFScanFunction {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let listing_scan_function = ScanFunction::try_from(exprs)?;

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_scan_function.listing_table_url.as_ref())
                .await
        })?;

        let schema = new_gff_schema_builder().build();

        let listing_table_options =
            ListingGFFTableOptions::new(listing_scan_function.file_compression_type)
                .with_indexed(false);

        let listing_table_config = ExonListingConfig::new_with_options(
            listing_scan_function.listing_table_url,
            listing_table_options,
        );

        let listing_table = ListingGFFTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}

/// A table function that returns a table provider for an indexed GFF file.
pub struct GFFIndexedScanFunction {
    ctx: SessionContext,
}

impl GFFIndexedScanFunction {
    /// Create a new `GFFIndexedScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

impl TableFunctionImpl for GFFIndexedScanFunction {
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

        let listing_table_options = ListingGFFTableOptions::new(FileCompressionType::GZIP)
            .with_indexed(true)
            .with_region(region);

        let listing_table_config =
            ExonListingConfig::new_with_options(listing_table_url, listing_table_options);

        let schema = new_gff_schema_builder().build();
        let listing_table = ListingGFFTable::new(listing_table_config, schema);

        Ok(Arc::new(listing_table))
    }
}
