use std::sync::Arc;

use datafusion::{
    common::FileCompressionType,
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::bgzf::{self};
use tokio_util::io::StreamReader;

use crate::datasources::vcf::{indexed_async_record_stream::AsyncBatchStream, VCFConfig};

/// A file opener for VCF files.
#[derive(Debug)]
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
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();

        match self.file_compression_type {
            FileCompressionType::GZIP => Ok(Box::pin(async move {
                let s = config.object_store.get(file_meta.location()).await?;
                let s = s.into_stream();

                let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                let stream_reader = StreamReader::new(stream_reader);

                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                let mut vcf_reader = noodles::vcf::AsyncReader::new(bgzf_reader);

                let header = vcf_reader.read_header().await?;
                let batch_stream = AsyncBatchStream::new(vcf_reader, config, Arc::new(header));

                Ok(batch_stream.into_stream().boxed())
            })),
            FileCompressionType::UNCOMPRESSED => Ok(Box::pin(async move {
                let s = config.object_store.get(file_meta.location()).await?;
                let s = s.into_stream();

                let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                let stream_reader = StreamReader::new(stream_reader);

                let mut vcf_reader = noodles::vcf::AsyncReader::new(stream_reader);
                let header = vcf_reader.read_header().await?;

                let batch_stream = AsyncBatchStream::new(vcf_reader, config, Arc::new(header));

                Ok(batch_stream.into_stream().boxed())
            })),
            _ => Err(DataFusionError::NotImplemented(
                "Only uncompressed and gzip compressed VCF files are supported".to_string(),
            )),
        }
    }
}
