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

use std::sync::Arc;

use datafusion::{
    datasource::physical_plan::{FileMeta, FileOpener},
    error::DataFusionError,
};
use exon_cram::{CRAMConfig, IndexedAsyncBatchStream};
use futures::StreamExt;
use object_store::buffered;

use crate::datasources::cram::index::CRAMIndexData;

pub(super) struct IndexedCRAMOpener {
    /// The base configuration for the file opener.
    config: Arc<CRAMConfig>,
}

impl IndexedCRAMOpener {
    /// Create a new indexed CRAM opener.
    pub fn new(config: Arc<CRAMConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for IndexedCRAMOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        let config = Arc::clone(&self.config);

        Ok(Box::pin(async move {
            let FileMeta {
                extensions,
                object_meta,
                range: _,
            } = file_meta;

            let index_record = extensions
                .as_ref()
                .and_then(|ext| ext.downcast_ref::<CRAMIndexData>())
                .ok_or(DataFusionError::Execution(
                    "Invalid index offsets".to_string(),
                ))?;

            tracing::info!(
                offset = index_record.offset,
                path = ?object_meta.location,
                "Reading CRAM file with offset and landmark",
            );

            let buf_reader =
                buffered::BufReader::new(Arc::clone(&config.object_store), &object_meta);

            let mut cram_reader = noodles::cram::AsyncReader::new(buf_reader);

            cram_reader
                .seek(std::io::SeekFrom::Start(index_record.offset))
                .await?;

            let batch_stream = IndexedAsyncBatchStream::try_new(
                cram_reader,
                index_record.header.clone(),
                config,
                index_record.records.clone(),
            )
            .await?
            .into_stream();

            Ok(batch_stream.boxed())
        }))
    }
}
