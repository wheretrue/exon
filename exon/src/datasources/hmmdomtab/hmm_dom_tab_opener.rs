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

use std::{sync::Arc, task::Poll};

use bytes::{Buf, Bytes};

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{ready, StreamExt, TryStreamExt};

use super::hmm_dom_tab_config::{build_hmm_dom_tab_decoder, HMMDomTabConfig};

/// Implements a datafusion `FileOpener` for HMMDomTab files.
pub struct HMMDomTabOpener {
    config: Arc<HMMDomTabConfig>,
    file_compression_type: FileCompressionType,
}

impl HMMDomTabOpener {
    /// Create a new HMMDomTab file opener.
    pub fn new(config: Arc<HMMDomTabConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for HMMDomTabOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let gff_config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();
        let mut decoder = build_hmm_dom_tab_decoder();
        let projection = self.config.projection.clone();

        Ok(Box::pin(async move {
            let get_result = gff_config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));

            let mut input = match file_compression_type.convert_stream(stream_reader) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            // This is modified from datafusion's CSV reader.
            let mut buffered = Bytes::new();

            let s = futures::stream::poll_fn(move |cx| {
                loop {
                    if buffered.is_empty() {
                        match ready!(input.poll_next_unpin(cx)) {
                            Some(Ok(b)) => buffered = b,
                            Some(Err(e)) => return Poll::Ready(Some(Err(e.into()))),
                            None => {}
                        };
                    }
                    let decoded = match decoder.decode(buffered.as_ref()) {
                        Ok(0) => break,
                        Ok(decoded) => decoded,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    };
                    buffered.advance(decoded);
                }

                let batch = match decoder.flush() {
                    Ok(None) => None,
                    Ok(Some(decoded)) => match &projection {
                        Some(p) => Some(decoded.project(&p)),
                        None => Some(Ok(decoded)),
                    },
                    Err(e) => Some(Err(e)),
                };

                Poll::Ready(batch)
            });
            Ok(s.boxed())
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
        datasources::hmmdomtab::{HMMDomTabConfig, HMMDomTabOpener},
        tests::test_listing_table_dir,
    };

    #[tokio::test]
    async fn test_opener() {
        let object_store = Arc::new(LocalFileSystem::new());

        let config = HMMDomTabConfig::new(object_store.clone());

        let opener = HMMDomTabOpener::new(Arc::new(config), FileCompressionType::UNCOMPRESSED);

        let path = test_listing_table_dir("hmmdomtab", "test.hmmdomtab");
        let object_meta = object_store.head(&path).await.unwrap();
        let file_meta = FileMeta::from(object_meta);

        let mut opened_file = opener.open(file_meta).unwrap().await.unwrap();

        let mut n_records = 0;
        while let Some(batch) = opened_file.next().await {
            let batch = batch.unwrap();
            n_records += batch.num_rows();
        }

        assert_eq!(n_records, 100);
    }
}
