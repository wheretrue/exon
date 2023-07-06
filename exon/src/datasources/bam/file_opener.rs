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
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::core::Region;
use object_store::GetResult;
use tokio_util::io::StreamReader;

use super::{
    batch_reader::{BatchReader, StreamRecordBatchAdapter},
    config::BAMConfig,
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

impl FileOpener for BAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();
        let region = self.region.clone();

        Ok(Box::pin(async move {
            match config.object_store.get(file_meta.location()).await? {
                GetResult::File(file, path) => match region {
                    Some(region) => {
                        let mut reader = noodles::bam::indexed_reader::Builder::default()
                            .build_from_path(path)?;

                        let header = reader.read_header()?;

                        let query = reader
                            .query(&header, &region)?
                            .map(Ok)
                            .collect::<io::Result<Vec<_>>>()?;

                        let record_stream = futures::stream::iter(query).boxed();

                        let batch_adapter =
                            StreamRecordBatchAdapter::new(record_stream, header, config.clone());

                        let batch_stream = batch_adapter.into_stream();

                        Ok(batch_stream.boxed())
                    }
                    None => {
                        let mut reader = noodles::bam::Reader::new(file);

                        let header = reader.read_header()?;

                        let record_iterator = RecordIterator::new(reader, header.clone()).unwrap();
                        let record_stream = futures::stream::iter(record_iterator).boxed();

                        let batch_adapter =
                            StreamRecordBatchAdapter::new(record_stream, header, config.clone());

                        let batch_stream = batch_adapter.into_stream();

                        Ok(batch_stream.boxed())
                    }
                },
                GetResult::Stream(s) => {
                    let stream_reader = Box::pin(s.map_err(DataFusionError::from));

                    let stream_reader = StreamReader::new(stream_reader);
                    let batch_reader = BatchReader::new(stream_reader, config).await?;

                    let batch_stream = batch_reader.into_stream();

                    Ok(batch_stream.boxed())
                }
            }
        }))
    }
}
