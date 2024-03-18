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
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use exon_cram::{AsyncBatchStream, CRAMConfig};
use futures::{StreamExt, TryStreamExt};
use noodles::sam::Header;
use tokio_util::io::StreamReader;

#[derive(Debug)]
pub struct CRAMOpener {
    /// The configuration for the CRAM file.
    config: Arc<CRAMConfig>,
}

impl CRAMOpener {
    /// Create a new CRAM opener.
    pub fn new(config: Arc<CRAMConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for CRAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();

        Ok(Box::pin(async move {
            let s = config.object_store.get(file_meta.location()).await?;

            let s = s.into_stream().map_err(DataFusionError::from);

            let stream_reader = Box::pin(s);
            let stream_reader = StreamReader::new(stream_reader);

            let mut cram_reader = noodles::cram::AsyncReader::new(stream_reader);
            cram_reader.read_file_definition().await?;

            let header = cram_reader.read_file_header().await?;
            let header: Header = header.parse().map_err(|_| {
                DataFusionError::Execution("Failed to parse CRAM header".to_string())
            })?;

            let batch_stream =
                AsyncBatchStream::try_new(cram_reader, header, config)?.into_stream();

            Ok(batch_stream.boxed())
        }))
    }
}
