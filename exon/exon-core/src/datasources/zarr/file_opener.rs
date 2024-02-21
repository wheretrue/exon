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

use datafusion::datasource::physical_plan::FileOpener;
use exon_zarr::{BatchReader, ZarrConfig};
use object_store::GetResultPayload;
use std::sync::Arc;
use futures::StreamExt;

pub struct ZarrOpener {
    /// The base config for the Zarr file.
    config: Arc<ZarrConfig>,
}

impl ZarrOpener {
    /// Create a new ZarrOpener from a ZarrConfig.
    pub fn new(config: Arc<ZarrConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for ZarrOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        let zarr_config = self.config.clone();

        use futures::TryStreamExt;
        use arrow::error::ArrowError;

        Ok(Box::pin(async move {
            match zarr_config
                .object_store
                .get(file_meta.location())
                .await?
                .payload
            {
                GetResultPayload::File(_, p) => {
                    let zarr_batch_reader = BatchReader::new(p, zarr_config);
                    let stream = zarr_batch_reader.into_stream().map_err(ArrowError::from);

                    Ok(stream.boxed())
                }
                GetResultPayload::Stream(_) => {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "ZarrOpener::open: unexpected stream".to_string(),
                    ))
                }
            }
        }))
    }
}
