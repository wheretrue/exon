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
use object_store::GetOptions;
use tokio_util::io::StreamReader;

use crate::{
    datasources::vcf::{indexed_async_batch_stream::IndexedAsyncBatchStream, VCFConfig},
    error::ExonError,
    streaming_bgzf::AsyncBGZFReader,
};

/// A file opener for VCF files.
#[derive(Debug)]
pub struct IndexedVCFOpener {
    /// The configuration for the opener.
    config: Arc<VCFConfig>,

    /// The region to use for opening the file.
    region: Arc<Region>,
}

impl IndexedVCFOpener {
    /// Create a new VCF file opener.
    pub fn new(config: Arc<VCFConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexedVCFOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        tracing::debug!("Opening file: {:?}", file_meta.location());

        let config = self.config.clone();
        let region = self.region.clone();

        Ok(Box::pin(async move {
            let s = config
                .object_store
                .get(file_meta.location())
                .await?
                .into_stream();

            let stream_reader = Box::pin(s.map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);

            let first_bgzf_reader = bgzf::AsyncReader::new(stream_reader);

            let mut vcf_reader = noodles::vcf::AsyncReader::new(first_bgzf_reader);

            // We save this header for later to pass to the batch reader for record deserialization.
            let header = vcf_reader.read_header().await?;

            let header_offset = vcf_reader.virtual_position();

            let batch_stream = match file_meta.range {
                Some(FileRange { start, end }) => {
                    // The ranges are actually virtual positions in the bgzf file.
                    let vp_start = VirtualPosition::from(start as u64);
                    let vp_end = VirtualPosition::from(end as u64);

                    if vp_end.compressed() == 0 {
                        // If the compressed end is 0, we want to read the entire file.
                        // Moreover, we need to seek to the header offset as the start is also 0.

                        let stream = config
                            .object_store
                            .get(file_meta.location())
                            .await?
                            .into_stream()
                            .map_err(DataFusionError::from);

                        let stream_reader = StreamReader::new(Box::pin(stream));

                        let mut async_reader = AsyncBGZFReader::from_reader(stream_reader);
                        async_reader.scan_to_virtual_position(header_offset).await?;

                        let bgzf_reader = async_reader.into_inner();

                        let vcf_reader = noodles::vcf::AsyncReader::new(bgzf_reader);

                        IndexedAsyncBatchStream::new(vcf_reader, config, Arc::new(header), region)
                    } else {
                        // Otherwise, we read the compressed range from the object store.

                        let start = vp_start.compressed() as usize;
                        let end = if vp_start.compressed() == vp_end.compressed() {
                            file_meta.object_meta.size
                        } else {
                            vp_end.compressed() as usize
                        };

                        tracing::info!(
                            "Reading compressed range: {}..{} (uncompressed {}..{}) of {}",
                            vp_start.compressed(),
                            vp_end.compressed(),
                            start,
                            end,
                            file_meta.location()
                        );

                        let get_options = GetOptions {
                            range: Some(Range { start, end }),
                            ..Default::default()
                        };

                        let get_response = config
                            .object_store
                            .get_opts(file_meta.location(), get_options)
                            .await?;

                        let stream = get_response.into_stream().map_err(DataFusionError::from);
                        let stream_reader = StreamReader::new(Box::pin(stream));

                        let mut async_reader = AsyncBGZFReader::from_reader(stream_reader);

                        // If we're at the start of the file, we need to seek to the header offset.
                        if vp_start.compressed() == 0 && vp_start.uncompressed() == 0 {
                            tracing::debug!("Seeking to header offset: {:?}", header_offset);
                            async_reader.scan_to_virtual_position(header_offset).await?;
                        }

                        // If we're not at the start of the file, we need to seek to the uncompressed
                        // offset. The compressed offset is always 0 in this case because we're
                        // reading from a block boundary.
                        if vp_start.uncompressed() > 0 {
                            let marginal_start_vp =
                                VirtualPosition::try_from((0, vp_start.uncompressed()))
                                    .map_err(ExonError::from)?;

                            async_reader
                                .scan_to_virtual_position(marginal_start_vp)
                                .await?;
                        }

                        let bgzf_reader = async_reader.into_inner();

                        let vcf_reader = noodles::vcf::AsyncReader::new(bgzf_reader);

                        let mut batch_stream = IndexedAsyncBatchStream::new(
                            vcf_reader,
                            config,
                            Arc::new(header),
                            region,
                        );

                        if vp_start.compressed() == vp_end.compressed() {
                            batch_stream =
                                batch_stream.with_max_bytes(vp_end.uncompressed() as usize);
                        }

                        batch_stream
                    }
                }
                None => {
                    let get_stream = config
                        .object_store
                        .get(file_meta.location())
                        .await?
                        .into_stream()
                        .map_err(DataFusionError::from);

                    let stream_reader = StreamReader::new(Box::pin(get_stream));

                    let mut async_reader = AsyncBGZFReader::from_reader(stream_reader);

                    // If we're at the start of the file, we need to seek to the header offset.
                    if vcf_reader.virtual_position().compressed() == 0
                        && vcf_reader.virtual_position().uncompressed() == 0
                    {
                        tracing::debug!("Seeking to header offset: {:?}", header_offset);
                        async_reader.scan_to_virtual_position(header_offset).await?;
                    }

                    let bgzf_reader = async_reader.into_inner();

                    let vcf_reader = noodles::vcf::AsyncReader::new(bgzf_reader);

                    IndexedAsyncBatchStream::new(vcf_reader, config, Arc::new(header), region)
                }
            };

            let batch_stream = batch_stream.into_stream();
            Ok(batch_stream.boxed())
        }))
    }
}
