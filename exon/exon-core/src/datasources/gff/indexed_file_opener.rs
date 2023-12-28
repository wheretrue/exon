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
    datasource::{listing::FileRange, physical_plan::FileOpener},
    error::DataFusionError,
};
use exon_gff::{BatchReader, GFFConfig};
use futures::{StreamExt, TryStreamExt};
use noodles::core::Region;
use noodles_bgzf::VirtualPosition;
use object_store::GetOptions;
use tokio_util::io::StreamReader;

use crate::{error::ExonError, streaming_bgzf::AsyncBGZFReader};

#[derive(Debug)]
pub struct IndexGffOpener {
    /// The configuration for the opener.
    config: Arc<GFFConfig>,

    /// The region to use for filtering.
    region: Arc<Region>,
}

impl IndexGffOpener {
    /// Create a new VCF file opener.
    pub fn new(config: Arc<GFFConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexGffOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        tracing::info!("Opening file: {:?}", file_meta.location());

        let config = self.config.clone();

        // may need to have this do post query, i.e. pass this down
        let region = self.region.clone();

        Ok(Box::pin(async move {
            let batch_stream = match file_meta.range {
                Some(FileRange { start, end }) => {
                    // The ranges are actually virtual positions
                    let vp_start = VirtualPosition::from(start as u64);
                    let vp_end = VirtualPosition::from(end as u64);

                    if vp_end.compressed() == 0 {
                        return Err(DataFusionError::Execution(
                            "Invalid file range specified".to_string(),
                        ));
                    } else {
                        let start = vp_start.compressed() as usize;
                        let end = if vp_start.compressed() == vp_end.compressed() {
                            file_meta.object_meta.size
                        } else {
                            vp_end.compressed() as usize
                        };

                        tracing::warn!(
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

                        BatchReader::new(bgzf_reader, config)
                            .with_region(region)
                            .into_stream()
                    }
                }
                None => {
                    return Err(DataFusionError::Execution(
                        "No file range specified".to_string(),
                    ))
                }
            };

            Ok(batch_stream.boxed())
        }))
    }
}
