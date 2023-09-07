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
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{
    bam,
    bgzf::{self},
    sam::Header,
};
use object_store::ObjectStore;
use tokio_util::io::StreamReader;

use crate::datasources::bam::{
    lazy_record_stream::{LazyBatchReader, LazyRecordIterator},
    BAMConfig,
};

/// Implements a datafusion `FileOpener` for BAM files.
pub struct UnIndexedBAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,
}

impl UnIndexedBAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for UnIndexedBAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();

        Ok(Box::pin(async move {
            let get_result = config
                .object_store
                .get(file_meta.location())
                .await?
                .into_stream();

            let stream_reader =
                StreamReader::new(Box::pin(get_result.map_err(DataFusionError::from)));

            let bgzf_reader = bgzf::AsyncReader::new(stream_reader);

            let mut bam_reader = bam::AsyncReader::from(bgzf_reader);

            let header = bam_reader.read_header().await?;
            let header: Header = header.parse().map_err(|_| {
                DataFusionError::Execution("Failed to parse BAM header: {}".to_string())
            })?;

            let record_iterator = LazyRecordIterator::new(bam_reader).into_stream().boxed();

            let batch_stream = LazyBatchReader::new(record_iterator, Arc::new(header), config)
                .into_stream()
                .boxed();

            Ok(batch_stream)
        }))
    }
}
