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
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::MzMLConfig};

/// Implements a datafusion `FileOpener` for MzML files.
pub struct MzMLOpener {
    /// The base configuration for the file scan.
    config: Arc<MzMLConfig>,

    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl MzMLOpener {
    /// Create a new MzML file opener.
    pub fn new(config: Arc<MzMLConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for MzMLOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let mzml_config = self.config.clone();
        let file_compression_type = self.file_compression_type;

        Ok(Box::pin(async move {
            let get_result = mzml_config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));

            let new_reader = match file_compression_type.convert_stream(stream_reader) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            let stream_reader = StreamReader::new(new_reader);

            let mzml_batch_reader = BatchReader::new(stream_reader, mzml_config).into_stream();

            Ok(mzml_batch_reader.boxed())
        }))
    }
}
