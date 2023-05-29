use std::sync::Arc;

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::GFFConfig};

/// Implements a datafusion `FileOpener` for GFF files.
pub struct GFFOpener {
    config: Arc<GFFConfig>,
    file_compression_type: FileCompressionType,
}

impl GFFOpener {
    /// Create a new GFF file opener.
    pub fn new(config: Arc<GFFConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for GFFOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let gff_config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

        Ok(Box::pin(async move {
            let get_result = gff_config.object_store.get(file_meta.location()).await?;

            let stream_reader = get_result.into_stream().map_err(DataFusionError::from);
            let stream_reader = Box::pin(stream_reader);

            let new_reader = file_compression_type.convert_stream(stream_reader).unwrap();

            let stream_reader = StreamReader::new(new_reader);

            let gff_batch_reader = BatchReader::new(stream_reader, gff_config).into_stream();

            Ok(gff_batch_reader.boxed())
        }))
    }
}
