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
        listing::FileRange,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::bgzf::{self, VirtualPosition};
use tokio_util::io::StreamReader;

use crate::datasources::vcf::{
    batch_reader::{BatchReader, UnIndexedRecordIterator},
    VCFConfig,
};

/// A file opener for VCF files.
#[derive(Debug)]
pub struct IndexedVCFOpener {
    /// The configuration for the opener.
    config: Arc<VCFConfig>,
}

impl IndexedVCFOpener {
    /// Create a new VCF file opener.
    pub fn new(config: Arc<VCFConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for IndexedVCFOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        tracing::debug!("Opening file: {:?}", file_meta.location());

        let config = self.config.clone();

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

            let bgzf_reader = match file_meta.range {
                Some(FileRange { start, end }) => {
                    // The ranges are actually virtual positions in the bgzf file.
                    let vp_start = VirtualPosition::from(start as u64);
                    let vp_end = VirtualPosition::from(end as u64);

                    if vp_end.compressed() == 0 {
                        // If the compressed end is 0, we want to read the entire file.
                        // Moreover, we need to seek to the header offset as the start is also 0.

                        let bytes = config
                            .object_store
                            .get(file_meta.location())
                            .await?
                            .bytes()
                            .await?;

                        let cursor = std::io::Cursor::new(bytes);
                        let mut bgzf_reader = bgzf::Reader::new(cursor);

                        bgzf_reader.seek(header_offset)?;

                        bgzf_reader
                    } else {
                        // Otherwise, we read the compressed range from the object store.
                        tracing::debug!(
                            "Reading compressed range: {}..{} of {}",
                            vp_start.compressed(),
                            vp_end.compressed(),
                            file_meta.location()
                        );

                        let end = if vp_start.compressed() == vp_end.compressed() {
                            file_meta.object_meta.size
                        } else {
                            vp_end.compressed() as usize
                        };

                        let bytes = config
                            .object_store
                            .get_range(
                                file_meta.location(),
                                std::ops::Range {
                                    start: vp_start.compressed() as usize,
                                    end,
                                },
                            )
                            .await?;

                        if bytes.is_empty() {
                            // Should never happen.
                            return Err(DataFusionError::Execution(
                                "Empty range read from object store".to_string(),
                            ));
                        }

                        let cursor = std::io::Cursor::new(bytes);
                        let mut bgzf_reader = bgzf::Reader::new(cursor);

                        // If we're at the start of the file, we need to seek to the header offset.
                        if vp_start.compressed() == 0 && vp_start.uncompressed() == 0 {
                            tracing::debug!("Seeking to header offset: {:?}", header_offset);
                            bgzf_reader.seek(header_offset)?;
                        }

                        // If we're not at the start of the file, we need to seek to the uncompressed
                        // offset. The compressed offset is always 0 in this case because we're
                        // reading from a block boundary.
                        if vp_start.uncompressed() > 0 {
                            let marginal_start_vp =
                                VirtualPosition::try_from((0, vp_start.uncompressed())).unwrap();
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

                    bgzf_reader.seek(header_offset)?;

                    bgzf_reader
                }
            };

            let vcf_reader = noodles::vcf::Reader::new(bgzf_reader);

            let record_iterator = UnIndexedRecordIterator::new(vcf_reader);

            let boxed_iter = Box::new(record_iterator);

            let batch_reader = BatchReader::new(boxed_iter, config, Arc::new(header));
            let batch_stream = futures::stream::iter(batch_reader);

            Ok(batch_stream.boxed())
        }))
    }
}
