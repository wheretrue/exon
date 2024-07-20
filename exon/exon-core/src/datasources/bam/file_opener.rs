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
use exon_bam::{BAMConfig, BatchReader};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

/// Implements a datafusion `FileOpener` for BAM files.
pub struct BAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,
}

impl BAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for BAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = Arc::clone(&self.config);

        Ok(Box::pin(async move {
            let get_request = config.object_store.get(file_meta.location()).await?;

            let get_stream = get_request.into_stream();
            let stream_reader = Box::pin(get_stream.map_err(DataFusionError::from));
            let stream_reader = StreamReader::new(stream_reader);

            let reader = BatchReader::new(stream_reader, config).await?;

            Ok(reader.into_stream().boxed())
        }))
    }
}
