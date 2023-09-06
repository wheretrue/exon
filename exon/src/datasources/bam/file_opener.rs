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

use std::{io, sync::Arc};

use datafusion::{
    datasource::{
        listing::FileRange,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{
    bam,
    bgzf::{self, VirtualPosition},
    core::Region,
    sam::Header,
};
use object_store::{GetOptions, GetResult, ObjectStore};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

use super::{
    batch_reader::{BatchReader, StreamRecordBatchAdapter},
    config::BAMConfig,
    lazy_record_stream::{LazyBatchReader, LazyRecordIterator},
    record_stream::RecordIterator,
};

/// Implements a datafusion `FileOpener` for BAM files.
pub struct BAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,

    /// An optional region to filter on.
    region: Option<Region>,
}

impl BAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>) -> Self {
        Self {
            config,
            region: None,
        }
    }

    /// Set the region to filter on.
    pub fn with_region(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }
}

// Given the file meta, read just the header and return it along with the size of the header
async fn get_header_and_size(
    object_store: Arc<dyn ObjectStore>,
    file_meta: &FileMeta,
) -> Result<(Header, VirtualPosition), DataFusionError> {
    let get_result = object_store.get(file_meta.location()).await?;

    match get_result {
        GetResult::File(file, _) => {
            let mut reader = noodles::bam::Reader::new(file);

            let header = reader.read_header()?;

            let vp = reader.virtual_position();

            Ok((header, vp))
        }
        GetResult::Stream(s) => {
            let stream_reader = Box::pin(s.map_err(DataFusionError::from));

            let stream_reader = StreamReader::new(stream_reader);
            let mut reader = noodles::bam::AsyncReader::new(stream_reader);

            let header = reader.read_header().await?;
            let header: Header = header.parse().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("invalid header: {e}"),
                )
            })?;

            let vp = reader.virtual_position();

            Ok((header, vp))
        }
    }
}

impl FileOpener for BAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();
        let region = self.region.clone();

        match region {
            Some(region) => Ok(Box::pin(async move {
                // Maybe don't need to do this in the no range case :shrug:
                let (header, vp) =
                    get_header_and_size(config.object_store.clone(), &file_meta).await?;

                let bgzf_reader = match file_meta.range {
                    Some(FileRange { start: _, end }) if end == 0 => {
                        let get_result = config
                            .object_store
                            .get(file_meta.location())
                            .await?
                            .into_stream();

                        let mut stream_reader =
                            StreamReader::new(Box::pin(get_result.map_err(DataFusionError::from)));

                        let mut bgzf_reader = bgzf::AsyncReader::new(stream_reader);

                        bgzf_reader
                    }
                    Some(FileRange { start, end }) => {
                        let mut get_options = GetOptions::default();
                        get_options.range = Some(std::ops::Range {
                            start: start as usize,
                            end: end as usize,
                        });

                        let get_result = config
                            .object_store
                            .get_opts(file_meta.location(), get_options)
                            .await?
                            .into_stream();

                        let mut stream_reader =
                            StreamReader::new(Box::pin(get_result.map_err(DataFusionError::from)));

                        let mut bgzf_reader = bgzf::AsyncReader::new(stream_reader);

                        bgzf_reader
                    }
                    None => {
                        let get_result = config
                            .object_store
                            .get(file_meta.location())
                            .await?
                            .into_stream();

                        let mut stream_reader =
                            StreamReader::new(Box::pin(get_result.map_err(DataFusionError::from)));

                        let mut bgzf_reader = bgzf::AsyncReader::new(stream_reader);

                        bgzf_reader
                    }
                };

                let bam_reader = bam::AsyncReader::from(bgzf_reader);
                let record_iterator = LazyRecordIterator::new(bam_reader).into_stream().boxed();

                let batch_stream = LazyBatchReader::new(record_iterator, header, config)
                    .into_stream()
                    .boxed();

                Ok(batch_stream)
            })),
            None => panic!("no region"),
        }
    }
}
