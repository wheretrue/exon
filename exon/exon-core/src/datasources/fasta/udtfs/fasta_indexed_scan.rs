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

use std::{str::FromStr, sync::Arc};

use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, function::TableFunctionImpl,
        listing::ListingTableUrl, TableProvider,
    },
    error::{DataFusionError, Result as DataFusionResult},
    execution::context::SessionContext,
    logical_expr::Expr,
    scalar::ScalarValue,
};
use exon_fasta::FASTASchemaBuilder;
use noodles::core::Region;
use object_store::{path::Path, ObjectStore};

use crate::{
    config::ExonConfigExtension,
    datasources::{
        exon_listing_table_options::ExonListingConfig,
        fasta::table_provider::{ListingFASTATable, ListingFASTATableOptions},
    },
    error::ExonError,
    physical_plan::object_store::{parse_url, url_to_object_store_url},
    ExonRuntimeEnvExt,
};

/// A table function that returns a table provider for an indexed FASTA file.
pub struct FastaIndexedScanFunction {
    ctx: SessionContext,
}

impl FastaIndexedScanFunction {
    /// Create a new `FastaIndexedScanFunction`.
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

/// Implement the `TableFunctionImpl` trait for `FastaIndexedScanFunction`.
impl TableFunctionImpl for FastaIndexedScanFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let Some(Expr::Literal(ScalarValue::Utf8(Some(path)))) = exprs.first() else {
            return Err(DataFusionError::Internal(
                "this function requires the path to be specified as the first argument".into(),
            ));
        };

        let Some(Expr::Literal(ScalarValue::Utf8(Some(region_str)))) = exprs.get(1) else {
            return Err(DataFusionError::Internal(
                "this function requires the region to be specified as the second argument".into(),
            ));
        };

        let listing_table_url = ListingTableUrl::parse(path)?;

        let passed_compression_type = exprs.get(2).and_then(|e| match e {
            Expr::Literal(ScalarValue::Utf8(Some(ref compression_type))) => {
                FileCompressionType::from_str(compression_type).ok()
            }
            _ => None,
        });

        let compression_type = listing_table_url
            .prefix()
            .extension()
            .and_then(|ext| FileCompressionType::from_str(ext).ok())
            .or(passed_compression_type)
            .unwrap_or(FileCompressionType::UNCOMPRESSED);

        let state = self.ctx.state();

        let exon_settings = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or(DataFusionError::Execution(
                "Exon settings must be configured.".to_string(),
            ))?;

        let fasta_schema = FASTASchemaBuilder::default()
            .with_large_utf8(exon_settings.fasta_large_utf8)
            .build();

        futures::executor::block_on(async {
            self.ctx
                .runtime_env()
                .exon_register_object_store_url(listing_table_url.as_ref())
                .await
        })?;

        let region_file_check = futures::executor::block_on(async {
            let region_url = parse_url(region_str.as_str())?;
            let object_store_url = url_to_object_store_url(&region_url)?;

            let store = self.ctx.runtime_env().object_store(object_store_url)?;
            let region_path = Path::from_url_path(region_url.path())?;

            store.head(&region_path).await.map_err(ExonError::from)
        });

        let region = Region::from_str(region_str);

        let mut listing_table_options = ListingFASTATableOptions::new(compression_type);

        match (region_file_check, region) {
            (Ok(_), _) => {
                listing_table_options =
                    listing_table_options.with_region_file(region_str.to_string());
            }
            (Err(_), Ok(region)) => {
                listing_table_options = listing_table_options.with_region(vec![region]);
            }
            (Err(_), Err(_)) => {
                return Err(DataFusionError::Execution(
                    "Region file or region must be specified.".to_string(),
                ));
            }
        }

        let listing_table_config =
            ExonListingConfig::new_with_options(listing_table_url, listing_table_options);

        let listing_table = ListingFASTATable::try_new(listing_table_config, fasta_schema)?;

        Ok(Arc::new(listing_table))
    }
}
