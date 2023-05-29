use std::sync::Arc;

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::BEDConfig};

/// Implements a datafusion `FileOpener` for BED files.
pub struct BEDOpener {
    /// The configuration for the file scan.
    config: Arc<BEDConfig>,

    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl BEDOpener {
    /// Create a new BED file opener.
    pub fn new(config: Arc<BEDConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for BEDOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
            let uncompressed_reader = match file_compression_type.convert_stream(stream_reader) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            let stream_reader = StreamReader::new(uncompressed_reader);
            let batch_reader = BatchReader::new(stream_reader, config);

            let batch_stream = batch_reader.into_stream();

            Ok(batch_stream.boxed())
        }))
    }
}
