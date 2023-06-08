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
                _ => Err(DataFusionError::NotImplemented(format!(
                    "Unsupported file compression type {file_compression_type:?}"
                ))),
            }
        }))
    }
}
