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
use noodles::{bgzf::VirtualPosition, core::Region};
use object_store::GetOptions;
use tokio_util::io::StreamReader;

use crate::streaming_bgzf::AsyncBGZFReader;

use super::{async_batch_stream::AsyncBatchStream, config::BAMConfig};

/// Implements a datafusion `FileOpener` for BAM files.
pub struct IndexedBAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,
    // An optional region to filter on.
    region: Arc<Region>,
}

impl IndexedBAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexedBAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();
        // TODO: push down region
        let region = self.region.clone();

        Ok(Box::pin(async move {
            let get_request = config.object_store.get(file_meta.location()).await?;
            let get_stream = get_request.into_stream();

            let stream_reader = Box::pin(get_stream.map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);

            let mut first_bam_reader = noodles::bam::AsyncReader::new(stream_reader);

            first_bam_reader.read_header().await?;

            let reference_sequences = first_bam_reader.read_reference_sequences().await?;

            let header_offset = first_bam_reader.virtual_position();

            let bgzf_reader = match file_meta.range {
                Some(FileRange { start, end }) => {
                    let vp_start = VirtualPosition::from(start as u64);
                    let vp_end = VirtualPosition::from(end as u64);

                    if vp_end.compressed() == 0 {
                        let stream = config
                            .object_store
                            .get(file_meta.location())
                            .await?
                            .into_stream()
                            .map_err(DataFusionError::from);

                        let stream_reader = StreamReader::new(Box::pin(stream));

                        let mut async_reader = AsyncBGZFReader::from_reader(stream_reader);
                        async_reader.scan_to_virtual_position(header_offset).await?;

                        async_reader
                    } else {
                        let start = vp_start.compressed() as usize;
                        let end = if vp_start.compressed() == vp_end.compressed() {
                            tracing::debug!("Reading entire file because start == end");
                            file_meta.object_meta.size
                        } else {
                            vp_end.compressed() as usize
                        };

                        tracing::debug!(
                            "Reading compressed range: {}..{} of {}",
                            vp_start.compressed(),
                            vp_end.compressed(),
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
                        let async_reader = AsyncBGZFReader::from_reader(stream_reader);

                        tracing::debug!("Scanning to {} if necessary", vp_start.uncompressed());

                        async_reader
                    }
                }
                None => {
                    todo!()
                }
            };

            let bgzf_reader = bgzf_reader.into_inner();

            let bam_reader = noodles::bam::AsyncReader::from(bgzf_reader);

            let reference_sequences = Arc::new(reference_sequences);

            let batch_stream =
                AsyncBatchStream::try_new(bam_reader, config, reference_sequences, region)?;

            Ok(batch_stream.into_stream().boxed())
        }))
    }
}
