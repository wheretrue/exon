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

use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use exon_vcf::{AsyncBatchStream, VCFConfig};
use futures::{StreamExt, TryStreamExt};
use noodles::bgzf::{self};
use tokio_util::io::StreamReader;

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

        tracing::debug!(
            "Opening file: {:?} with compression {:?}",
            file_meta.location(),
            self.file_compression_type
        );

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
