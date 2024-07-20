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

use std::sync::Arc;

use arrow::error::ArrowError;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use exon_fastq::{BatchReader, FASTQConfig};
use futures::{StreamExt, TryStreamExt};
use tokio::io::AsyncBufReadExt;
use tokio_util::io::StreamReader;

use crate::streaming_bgzf::is_bgzip_valid_header;

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
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = Arc::clone(&self.config);
        let file_compression_type = self.file_compression_type;

        Ok(Box::pin(async move {
            match file_compression_type {
                FileCompressionType::GZIP => {
                    let get_result = config.object_store.get(file_meta.location()).await?;

                    let stream = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
                    let mut stream_reader = StreamReader::new(stream);

                    let buf = stream_reader.fill_buf().await?;

                    if is_bgzip_valid_header(buf) {
                        let get_result = config.object_store.get(file_meta.location()).await?;

                        let stream =
                            Box::pin(get_result.into_stream().map_err(DataFusionError::from));
                        let stream_reader = StreamReader::new(stream);
                        let bgzf_reader = noodles::bgzf::AsyncReader::new(stream_reader);
                        let batch_reader = BatchReader::new(bgzf_reader, config);

                        let batch_stream = batch_reader.into_stream().map_err(ArrowError::from);

                        Ok(batch_stream.boxed())
                    } else {
                        let get_result = config.object_store.get(file_meta.location()).await?;

                        let stream =
                            Box::pin(get_result.into_stream().map_err(DataFusionError::from));

                        let new_reader = file_compression_type.convert_stream(stream)?;
                        let buf_reader = StreamReader::new(new_reader);
                        let batch_reader = BatchReader::new(buf_reader, config);

                        let batch_stream = batch_reader.into_stream().map_err(ArrowError::from);

                        Ok(batch_stream.boxed())
                    }
                }
                _ => {
                    let get_result = config.object_store.get(file_meta.location()).await?;

                    let stream = Box::pin(get_result.into_stream().map_err(DataFusionError::from));

                    let new_reader = file_compression_type.convert_stream(stream)?;
                    let buf_reader = StreamReader::new(new_reader);
                    let batch_reader = BatchReader::new(buf_reader, config);

                    let batch_stream = batch_reader.into_stream().map_err(ArrowError::from);

                    Ok(batch_stream.boxed())
                }
            }
        }))
    }
}

#[cfg(test)]
mod test {}
