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
use exon_bcf::{BCFConfig, BatchAdapter, BatchReader};
use futures::{StreamExt, TryStreamExt};
use noodles::{bcf, core::Region, csi};
use object_store::GetResultPayload;
use tokio::io::BufReader;
use tokio_util::io::StreamReader;

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
        let config = Arc::clone(&self.config);
        let region = self.region.clone();

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            match get_result.payload {
                GetResultPayload::File(file, path) => match region {
                    Some(region) => {
                        let mut reader = bcf::io::Reader::new(file);
                        let header = reader.read_header()?;

                        let index = csi::read(path.with_extension("bcf.csi"))?;

                        let query = reader.query(&header, &index, &region)?;

                        let mut records = Vec::new();

                        for result in query {
                            records.push(result);
                        }

                        let boxed_iter = Box::new(records.into_iter());

                        let batch_adapter = BatchAdapter::new(boxed_iter, config, header.into());
                        let batch_stream = futures::stream::iter(batch_adapter);

                        Ok(batch_stream.boxed())
                    }
                    None => {
                        let buf_reader = tokio::fs::File::open(path).await.map(BufReader::new)?;
                        let batch_reader = BatchReader::new(buf_reader, config).await?;

                        let batch_stream = batch_reader.into_stream();

                        Ok(batch_stream.boxed())
                    }
                },
                GetResultPayload::Stream(s) => {
                    if region.is_some() {
                        return Err(DataFusionError::NotImplemented(
                            "region filtering is not yet implemented for object stores".to_string(),
                        ));
                    }

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
