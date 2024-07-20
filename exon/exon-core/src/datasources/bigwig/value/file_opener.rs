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
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener as FileOpenerTrait},
    error::DataFusionError,
};
use futures::StreamExt;
use object_store::GetResultPayload;

pub struct FileOpener {
    /// The configuration for the file scan.
    config: Arc<exon_bigwig::value_batch_reader::BigWigValueConfig>,
}

impl FileOpener {
    /// Create a new file opener.
    pub fn new(config: Arc<exon_bigwig::value_batch_reader::BigWigValueConfig>) -> Self {
        Self { config }
    }
}

impl FileOpenerTrait for FileOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = Arc::clone(&self.config);

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            match get_result.payload {
                GetResultPayload::File(_, path_buf) => {
                    let batch_reader =
                        exon_bigwig::value_batch_reader::ValueRecordBatchReader::try_new(
                            &path_buf.display().to_string(),
                            Arc::clone(&config),
                        )?;

                    let batch_stream = batch_reader.into_stream();

                    Ok(batch_stream.boxed())
                }
                _ => Err(DataFusionError::Execution(
                    "BigWig file opener expected a file".to_string(),
                )),
            }
        }))
    }
}
