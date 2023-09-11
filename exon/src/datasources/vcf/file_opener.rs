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
    common::FileCompressionType,
    datasource::{
        listing::FileRange,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{
    bgzf::{self, VirtualPosition},
    core::Region,
};
use tokio_util::io::StreamReader;

use super::{
    async_batch_reader::AsyncBatchReader,
    batch_reader::{BatchReader, UnIndexedRecordIterator},
    config::VCFConfig,
};

/// A file opener for VCF files.
#[derive(Debug)]
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

        match (region, file_compression_type) {
            (Some(_), FileCompressionType::GZIP) => Ok(Box::pin(async move {
                let s = config.object_store.get(file_meta.location()).await?;
                let s = s.into_stream();

                let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                let stream_reader = StreamReader::new(stream_reader);

                let first_bgzf_reader = bgzf::AsyncReader::new(stream_reader);

                let mut vcf_reader = noodles::vcf::AsyncReader::new(first_bgzf_reader);
                let header = vcf_reader.read_header().await?;

                let vp = vcf_reader.virtual_position();

                let bgzf_reader = match file_meta.range {
                    Some(FileRange { start, end }) => {
                        let vp_start = VirtualPosition::from(start as u64);
                        let vp_end = VirtualPosition::from(end as u64);

                        if vp_end.compressed() == 0 {
                            let bytes = config
                                .object_store
                                .get(file_meta.location())
                                .await?
                                .bytes()
                                .await?;

                            let cursor = std::io::Cursor::new(bytes);
                            let mut bgzf_reader = bgzf::Reader::new(cursor);

                            bgzf_reader.seek(vp)?;

                            bgzf_reader
                        } else {
                            let bytes = config
                                .object_store
                                .get_range(
                                    file_meta.location(),
                                    std::ops::Range {
                                        start: vp_start.compressed() as usize,
                                        end: vp_end.compressed() as usize,
                                    },
                                )
                                .await?;

                            let cursor = std::io::Cursor::new(bytes);
                            let mut bgzf_reader = bgzf::Reader::new(cursor);

                            if vp_start.compressed() == 0 {
                                bgzf_reader.seek(vp)?;
                            }

                            if vp_start.uncompressed() > 0 {
                                let marginal_start_vp =
                                    VirtualPosition::try_from((0, vp_start.uncompressed()))
                                        .unwrap();
                                bgzf_reader.seek(marginal_start_vp)?;
                            }

                            bgzf_reader
                        }
                    }
                    None => {
                        let bytes = config
                            .object_store
                            .get(file_meta.location())
                            .await?
                            .bytes()
                            .await?;

                        let cursor = std::io::Cursor::new(bytes);
                        let mut bgzf_reader = bgzf::Reader::new(cursor);

                        bgzf_reader.seek(vp)?;

                        bgzf_reader
                    }
                };

                let vcf_reader = noodles::vcf::Reader::new(bgzf_reader);

                let record_iterator = UnIndexedRecordIterator::new(vcf_reader);

                let boxed_iter = Box::new(record_iterator);

                let batch_reader = BatchReader::new(boxed_iter, config, Arc::new(header));

                let batch_stream = futures::stream::iter(batch_reader);

                Ok(batch_stream.boxed())
            })),
            (None, FileCompressionType::GZIP) => Ok(Box::pin(async move {
                let s = config.object_store.get(file_meta.location()).await?;
                let s = s.into_stream();

                let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                let stream_reader = StreamReader::new(stream_reader);

                let bgzf_reader = bgzf::AsyncReader::new(stream_reader);

                let record_iterator = AsyncBatchReader::new(bgzf_reader, config).await?;

                Ok(record_iterator.into_stream().boxed())
            })),
            (_, FileCompressionType::UNCOMPRESSED) => Ok(Box::pin(async move {
                let s = config.object_store.get(file_meta.location()).await?;
                let s = s.into_stream();

                let stream_reader = Box::pin(s.map_err(DataFusionError::from));
                let stream_reader = StreamReader::new(stream_reader);

                let batch_reader = AsyncBatchReader::new(stream_reader, config).await?;
                Ok(batch_reader.into_stream().boxed())
            })),
            (_, _) => Err(DataFusionError::NotImplemented(
                "Only uncompressed and gzip compressed VCF files are supported".to_string(),
            )),
        }
    }
}
