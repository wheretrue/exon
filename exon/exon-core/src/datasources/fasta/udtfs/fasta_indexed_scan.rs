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

use crate::{
    config::ExonConfigExtension,
    datasources::fasta::table_provider::{
        ListingFASTATable, ListingFASTATableConfig, ListingFASTATableOptions,
    },
    error::ExonError,
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

        let region: Region = region_str.parse().map_err(ExonError::from)?;

        let listing_table_options =
            ListingFASTATableOptions::new(compression_type).with_region(Arc::new(region));

        let listing_table_config =
            ListingFASTATableConfig::new(listing_table_url).with_options(listing_table_options);

        let listing_table = ListingFASTATable::try_new(listing_table_config, fasta_schema)?;

        Ok(Arc::new(listing_table))
    }
}
