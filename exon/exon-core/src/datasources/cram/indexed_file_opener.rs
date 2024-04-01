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

use datafusion::{datasource::physical_plan::FileOpener, error::DataFusionError};
use exon_cram::CRAMConfig;
use futures::TryStreamExt;
use noodles::{core::Region, cram::crai::Record};
use tokio_util::io::StreamReader;

pub(super) struct IndexedCRAMOpener {
    /// The base configuration for the file opener.
    config: Arc<CRAMConfig>,
    /// The specific region to open.
    region: Arc<Region>,
}

impl IndexedCRAMOpener {
    /// Create a new indexed CRAM opener.
    pub fn new(config: Arc<CRAMConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexedCRAMOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        let config = self.config.clone();
        let region = self.region.clone();

        Ok(Box::pin(async move {
            let get_request = config.object_store.get(file_meta.location()).await?;
            let get_stream = get_request.into_stream();

            let stream_reader = Box::pin(get_stream.map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);

            let index_record = file_meta
                .extensions
                .map(|e| {
                    let record = e.clone();
                    let idx_record = record.downcast_ref::<Record>().cloned();

                    idx_record
                })
                .flatten()
                .ok_or(DataFusionError::Execution(
                    "Could not find index record in file meta".to_string(),
                ))?;

            todo!()
        }))
    }
}
