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

use std::{ops::Range, sync::Arc};

use datafusion::{
    common::FileCompressionType,
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{bgzf, core::Region};
use object_store::GetResult;
use tokio_util::io::StreamReader;

use super::{
    async_batch_reader::AsyncBatchReader,
    batch_reader::{BatchReader, UnIndexedRecordIterator},
    config::VCFConfig,
};

/// A file opener for VCF files.
pub struct VCFOpener {
    /// The configuration for the opener.
    config: Arc<VCFConfig>,
    /// The file compression type.
    file_compression_type: FileCompressionType,
    /// The region to filter on.
    region: Option<Region>,
}

impl VCFOpener {
    /// Create a new VCF file opener.
    pub fn new(config: Arc<VCFConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
            region: None,
        }
    }

    /// Set the region to filter on.
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }
}

impl FileOpener for VCFOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();
        let region = self.region.clone();

        let file_compression_type = self.file_compression_type;

        if let Some(region) = &region {
            return Ok(Box::pin(async move {
                let s = config.object_store.get(file_meta.location()).await?;
                let s = s.into_stream();

                let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                let stream_reader = StreamReader::new(stream_reader);

                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                let mut vcf_reader = noodles::vcf::AsyncReader::new(bgzf_reader);

                let header = vcf_reader.read_header().await?;

                let byte_region = config
                    .object_store
                    .get_range(
                        file_meta.location(),
                        Range {
                            start: file_meta.range.clone().map(|r| r.start).unwrap_or(0) as usize,
                            end: file_meta.range.clone().map(|r| r.end).unwrap_or(0) as usize,
                        },
                    )
                    .await?;

                let cursor = std::io::Cursor::new(byte_region);
                let bgzf_reader = bgzf::Reader::new(cursor);
                // let vcf_reader = noodles::vcf::Reader::new(bgzf_reader);

                let record_iterator = UnIndexedRecordIterator::new_with_header(bgzf_reader, header);
                let boxed_iter = Box::new(record_iterator);

                let batch_reader = BatchReader::new(boxed_iter, config);

                let batch_stream = futures::stream::iter(batch_reader);

                Ok(batch_stream.boxed())
            }));
        }

        Ok(Box::pin(async move {
            match config.object_store.get(file_meta.location()).await? {
                GetResult::File(file, path) => {
                    let buf_reader = std::io::BufReader::new(file);

                    match (file_compression_type, region) {
                        (FileCompressionType::UNCOMPRESSED, None) => {
                            let record_iterator = UnIndexedRecordIterator::try_new(buf_reader)?;
                            let boxed_iter = Box::new(record_iterator);

                            let batch_reader = BatchReader::new(boxed_iter, config);

                            let batch_stream = futures::stream::iter(batch_reader);

                            Ok(batch_stream.boxed())
                        }
                        (FileCompressionType::GZIP, None) => {
                            let bgzf_reader = bgzf::Reader::new(buf_reader);

                            let record_iterator = UnIndexedRecordIterator::try_new(bgzf_reader)?;
                            let boxed_iter = Box::new(record_iterator);

                            let batch_reader = BatchReader::new(boxed_iter, config);

                            let batch_stream = futures::stream::iter(batch_reader);

                            Ok(batch_stream.boxed())
                        }
                        (FileCompressionType::GZIP, Some(region)) => {
                            panic!("Region filtering not supported for GZIP files")
                            // let mut reader = noodles::vcf::indexed_reader::Builder::default()
                            //     .build_from_path(path)?;

                            // let header = reader.read_header()?;

                            // let query = reader.query(&header, &region)?;
                            // let mut records = Vec::new();

                            // for result in query {
                            //     records.push(result);
                            // }

                            // let boxed_iter = Box::new(records.into_iter());

                            // let batch_reader = BatchReader::new(boxed_iter, config);
                            // let batch_stream = futures::stream::iter(batch_reader);

                            // Ok(batch_stream.boxed())
                        }
                        _ => Err(DataFusionError::NotImplemented(
                            "Unsupported file compression type".to_string(),
                        )),
                    }
                }
                GetResult::Stream(s) => {
                    todo!("GetResult::Stream")
                    // let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                    // let stream_reader = StreamReader::new(stream_reader);

                    // match file_compression_type {
                    //     FileCompressionType::UNCOMPRESSED => {
                    //         let batch_reader = AsyncBatchReader::new(stream_reader, config).await?;
                    //         Ok(batch_reader.into_stream().boxed())
                    //     }
                    //     FileCompressionType::GZIP => {
                    //         let bgzf_reader = bgzf::AsyncReader::new(stream_reader);
                    //         let batch_reader = AsyncBatchReader::new(bgzf_reader, config).await?;

                    //         Ok(batch_reader.into_stream().boxed())
                    //     }
                    //     _ => Err(DataFusionError::NotImplemented(format!(
                    //         "Unsupported file compression type {file_compression_type:?}"
                    //     ))),
                    // }
                }
            }
        }))
    }
}
