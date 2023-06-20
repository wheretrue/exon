use arrow::datatypes::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    datasource::{
        file_format::file_type::FileCompressionType,
        listing::{ListingOptions, ListingTableUrl},
    },
    error::DataFusionError,
    execution::{context::SessionState, options::ReadOptions},
    prelude::SessionConfig,
};

use super::ExonFileType;

#[derive(Clone)]
/// ExonReadOptions control how exon files are read.
pub struct ExonReadOptions<'a> {
    /// The type of exon file.
    pub exon_file_type: ExonFileType,

    /// The type of compression used for the file.
    pub file_compression_type: FileCompressionType,

    /// The schema of the file.
    pub schema: Option<&'a Schema>,
}

impl<'a> ExonReadOptions<'a> {
    /// Create a new `ExonReadOptions`.
    pub fn new(exon_file_type: ExonFileType) -> Self {
        Self {
            exon_file_type,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            schema: None,
        }
    }

    /// Set the compression type of the file.
    pub fn with_compression(mut self, file_compression_type: FileCompressionType) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }
}

#[async_trait]
impl ReadOptions<'_> for ExonReadOptions<'_> {
    fn to_listing_options(
        &self,
        _config: &datafusion::prelude::SessionConfig,
    ) -> datafusion::datasource::listing::ListingOptions {
        let file_format = self
            .exon_file_type
            .clone()
            .get_file_format(self.file_compression_type.clone());

        ListingOptions::new(file_format)
    }

    async fn get_resolved_schema(
        &self,
        config: &SessionConfig,
        state: SessionState,
        table_path: ListingTableUrl,
    ) -> Result<SchemaRef, DataFusionError> {
        self._get_resolved_schema(config, state, table_path, self.schema, false)
            .await
    }
}
