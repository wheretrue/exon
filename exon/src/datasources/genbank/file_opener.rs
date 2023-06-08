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
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{StreamExt, TryStreamExt};

use super::{batch_reader::BatchReader, config::GenbankConfig};

/// Implements a datafusion `FileOpener` for Genbank files.
pub struct GenbankOpener {
    config: Arc<GenbankConfig>,
    file_compression_type: FileCompressionType,
}

impl GenbankOpener {
    /// Create a new Genbank file opener.
    pub fn new(config: Arc<GenbankConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for GenbankOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            let stream_reader = get_result.into_stream().map_err(DataFusionError::from);
            let stream_reader = Box::pin(stream_reader);

            let new_reader = file_compression_type.convert_stream(stream_reader).unwrap();

            // Read all the bytes from the new stream
            let collected = new_reader
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>();

            let cursor = std::io::Cursor::new(collected);
            let buf_reader = std::io::BufReader::new(cursor);

            let gff_batch_reader = BatchReader::new(buf_reader, config).into_stream();

            Ok(gff_batch_reader.boxed())
        }))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{
        datasource::file_format::file_type::FileCompressionType,
        physical_plan::file_format::{FileMeta, FileOpener},
    };
    use futures::StreamExt;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::{
        datasources::genbank::{GenbankConfig, GenbankOpener},
        tests::test_listing_table_dir,
    };

    #[tokio::test]
    async fn test_opener() {
        let object_store = Arc::new(LocalFileSystem::new());

        let config = GenbankConfig::new(object_store.clone());
        let opener = GenbankOpener::new(Arc::new(config), FileCompressionType::UNCOMPRESSED);

        let path = test_listing_table_dir("genbank", "test.gb");

        let object_meta = object_store.head(&path).await.unwrap();
        let file_meta = FileMeta::from(object_meta);

        let mut opened_file = opener.open(file_meta).unwrap().await.unwrap();

        let mut n_records = 0;
        while let Some(batch) = opened_file.next().await {
            let batch = batch.unwrap();
            n_records += batch.num_rows();
        }

        assert_eq!(n_records, 1);
    }
}
