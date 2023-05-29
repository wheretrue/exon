use std::sync::Arc;

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{StreamExt, TryStreamExt};
use noodles::bgzf;
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::VCFConfig};

/// A file opener for VCF files.
pub struct VCFOpener {
    /// The configuration for the opener.
    config: Arc<VCFConfig>,
    /// The file compression type.
    file_compression_type: FileCompressionType,
}

impl VCFOpener {
    /// Create a new VCF file opener.
    pub fn new(config: Arc<VCFConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for VCFOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);

            match file_compression_type {
                FileCompressionType::UNCOMPRESSED => {
                    let batch_reader = BatchReader::new(stream_reader, config).await?;
                    Ok(batch_reader.into_stream().boxed())
                }
                FileCompressionType::GZIP => {
                    let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                    let batch_reader = BatchReader::new(bgzf_reader, config).await?;

                    Ok(batch_reader.into_stream().boxed())
                }
                _ => {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported file compression type {:?}",
                        file_compression_type
                    )))
                }
            }
        }))
    }
}
