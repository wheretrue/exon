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

use core::panic;
use std::{io::BufReader, sync::Arc};

use datafusion::datasource::{
    file_format::file_compression_type::FileCompressionType, physical_plan::FileOpener,
};
use exon_sdf::SDFConfig;
use futures::StreamExt;
use object_store::GetResultPayload;

#[derive(Debug)]
pub struct SDFOpener {
    /// The file compression type.
    file_compression_type: FileCompressionType,

    /// The configuration for the opener.
    config: Arc<SDFConfig>,
}

impl SDFOpener {
    /// Create a new SDF file opener.
    pub fn new(config: Arc<SDFConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for SDFOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type;

        Ok(Box::pin(async move {
            match file_compression_type {
                FileCompressionType::UNCOMPRESSED => {
                    let get_request = config.object_store.get(file_meta.location()).await?;

                    match get_request.payload {
                        GetResultPayload::File(f, _) => {
                            let reader = BufReader::new(f);

                            let batch_reader = exon_sdf::BatchReader::new(reader, config);

                            let stream = batch_reader.into_stream();

                            Ok(stream.boxed())
                        }
                        _ => panic!("Expected a file"),
                    }
                }
                _ => panic!("Unsupported compression type"),
            }
        }))
    }
}

// std::result::Result<Pin<Box<(dyn futures::Stream<Item = std::result::Result<arrow::array::RecordBatch, arrow::error::ArrowError>> + std::marker::Send + 'static)>>, DataFusionError>`
// std::result::Result<impl futures::Stream<Item = std::result::Result<arrow::array::RecordBatch, arrow::error::ArrowError>>, _>
