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
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{bcf, core::Region, csi};
use object_store::GetResult;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;

use crate::physical_plan::object_store::get_byte_region;

use super::{
    batch_reader::{BatchAdapter, BatchReader},
    config::BCFConfig,
    lazy_batch_reader::{LazyBCFBatchReader, LazyBCFRecordReader},
};

/// A file opener for BCF files.
pub struct BCFOpener {
    /// The configuration for the opener.
    config: Arc<BCFConfig>,

    /// The region to filter on.
    region: Option<Region>,
}

impl BCFOpener {
    /// Create a new BCF file opener.
    pub fn new(config: Arc<BCFConfig>) -> Self {
        Self {
            config,
            region: None,
        }
    }

    /// Set the region to filter on.
    pub fn with_region_filter(mut self, region: Region) -> Self {
        self.region = Some(region);
        self
    }
}

impl FileOpener for BCFOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();
        let region = self.region.clone();

        match region {
            Some(_) => Ok(Box::pin(async move {
                let byte_region = get_byte_region(&config.object_store, file_meta).await?;
                let cursor = std::io::Cursor::new(byte_region);

                let bgzf_reader = noodles::bgzf::Reader::new(cursor);

                let lazy_reader = LazyBCFRecordReader::new(bgzf_reader);
                let lazy_reader = Box::new(lazy_reader);

                let batch_reader = LazyBCFBatchReader::new(lazy_reader, config);

                let batch_stream = futures::stream::iter(batch_reader);

                Ok(batch_stream.boxed())
            })),
            None => Ok(Box::pin(async move {
                let s = config
                    .object_store
                    .get(file_meta.location())
                    .await?
                    .into_stream();
                let stream_reader = Box::pin(s.map_err(DataFusionError::from));

                let stream_reader = StreamReader::new(stream_reader);
                let batch_reader = BatchReader::new(stream_reader, config).await?;

                let batch_stream = batch_reader.into_stream();

                Ok(batch_stream.boxed())
            })),
        }
    }
}
