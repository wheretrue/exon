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

use arrow::error::ArrowError;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use exon_gff::{BatchReader, GFFConfig};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

/// Implements a datafusion `FileOpener` for GFF files.
pub struct GFFOpener {
    config: Arc<GFFConfig>,
    file_compression_type: FileCompressionType,
}

impl GFFOpener {
    /// Create a new GFF file opener.
    pub fn new(config: Arc<GFFConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for GFFOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let gff_config = Arc::clone(&self.config);
        let file_compression_type = self.file_compression_type;

        Ok(Box::pin(async move {
            let get_result = gff_config.object_store.get(file_meta.location()).await?;

            let stream_reader = get_result.into_stream().map_err(DataFusionError::from);
            let stream_reader = Box::pin(stream_reader);

            let new_reader = file_compression_type.convert_stream(stream_reader)?;

            let stream_reader = StreamReader::new(new_reader);

            let gff_batch_reader = BatchReader::new(stream_reader, gff_config)
                .into_stream()
                .map_err(ArrowError::from);

            Ok(gff_batch_reader.boxed())
        }))
    }
}
