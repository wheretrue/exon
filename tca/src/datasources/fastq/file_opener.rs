use std::sync::Arc;

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::FASTQConfig};

/// Implements a datafusion `FileOpener` for FASTQ files.
pub struct FASTQOpener {
    /// The base configuration for the file scan.
    config: Arc<FASTQConfig>,
    /// The file compression type for the file to scan.
    file_compression_type: FileCompressionType,
}

impl FASTQOpener {
    /// Create a new FASTQ file opener.
    pub fn new(config: Arc<FASTQConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for FASTQOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));

            let new_reader = match file_compression_type.convert_stream(stream_reader) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            let buf_reader = StreamReader::new(new_reader);
            let batch_reader = BatchReader::new(buf_reader, config);

            let batch_stream = batch_reader.into_stream();

            Ok(batch_stream.boxed())
        }))
    }
}

#[cfg(test)]
mod test {}
