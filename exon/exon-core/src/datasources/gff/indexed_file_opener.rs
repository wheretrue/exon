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

use std::{ops::Range, sync::Arc};

use arrow::error::ArrowError;
use datafusion::{datasource::physical_plan::FileOpener, error::DataFusionError};
use exon_gff::{BatchReader, GFFConfig};
use futures::{StreamExt, TryStreamExt};
use noodles::bgzf::VirtualPosition;
use noodles::core::Region;
use object_store::{GetOptions, GetRange};
use tokio_util::io::StreamReader;

use crate::{
    datasources::indexed_file::indexed_bgzf_file::BGZFIndexedOffsets, error::ExonError,
    streaming_bgzf::AsyncBGZFReader,
};

#[derive(Debug)]
pub struct IndexedGffOpener {
    /// The configuration for the opener.
    config: Arc<GFFConfig>,

    /// The region to use for filtering.
    region: Arc<Region>,
}

impl IndexedGffOpener {
    /// Create a new GFF file opener.
    pub fn new(config: Arc<GFFConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexedGffOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        tracing::info!("Opening file: {:?}", file_meta.location());

        let config = self.config.clone();

        // may need to have this do post query, i.e. pass this down
        let region = self.region.clone();

        Ok(Box::pin(async move {
            let index_offsets = file_meta
                .extensions
                .as_ref()
                .and_then(|ext| ext.downcast_ref::<BGZFIndexedOffsets>())
                .ok_or(DataFusionError::Execution(
                    "Invalid index offsets".to_string(),
                ))?;

            let vp_start = index_offsets.start;
            let vp_end = index_offsets.end;

            if vp_end.compressed() == 0 {
                return Err(DataFusionError::Execution(
                    "Invalid file range specified".to_string(),
                ));
            }

            let start = vp_start.compressed() as usize;
            let end = if vp_start.compressed() == vp_end.compressed() {
                file_meta.object_meta.size
            } else {
                vp_end.compressed() as usize
            };

            tracing::debug!(
                "Reading compressed range: {}..{} (uncompressed {}..{}) of {}",
                vp_start.compressed(),
                vp_end.compressed(),
                start,
                end,
                file_meta.location()
            );

            let get_options = GetOptions {
                range: Some(GetRange::Bounded(Range { start, end })),
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
                let marginal_start_vp = VirtualPosition::try_from((0, vp_start.uncompressed()))
                    .map_err(ExonError::from)?;

                async_reader
                    .scan_to_virtual_position(marginal_start_vp)
                    .await?;
            }

            let bgzf_reader = async_reader.into_inner();

            let batch_stream = BatchReader::new(bgzf_reader, config)
                .with_region(region)
                .into_stream()
                .map_err(ArrowError::from);

            Ok(batch_stream.boxed())
        }))
    }
}
