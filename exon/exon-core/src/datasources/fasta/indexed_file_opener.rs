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

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::Schema,
    error::ArrowError,
};
use bytes::Bytes;
use datafusion::{
    datasource::{
        file_format::file_compression_type::{self, FileCompressionType},
        physical_plan::FileOpener,
    },
    error::DataFusionError,
};
use exon_fasta::FASTAConfig;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{GetOptions, GetRange, GetResultPayload};

use crate::datasources::indexed_file::{fai::FAIFileRange, region::RegionObjectStoreExtension};

#[derive(Debug)]
pub struct IndexedFASTAOpener {
    /// The configuration for the opener.
    config: Arc<FASTAConfig>,

    /// The file compression type.
    file_compression_type: FileCompressionType,
}

impl IndexedFASTAOpener {
    pub fn new(config: Arc<FASTAConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

fn record_batch_stream(
    sequence_name: &str,
    sequence_literal: &[u8],
    record_schema: Arc<Schema>,
) -> impl Stream<Item = arrow::error::Result<RecordBatch>> {
    // Create a single record batch with the sequence literal.
    let record_batch = RecordBatch::try_new(
        Arc::clone(&record_schema),
        vec![
            Arc::new(StringArray::from(vec![String::from(sequence_name)])),
            Arc::new(StringArray::from(Vec::<Option<String>>::from([None]))),
            Arc::new(StringArray::from(vec![String::from_utf8_lossy(
                sequence_literal,
            )
            .to_string()])),
        ],
    );

    let s = futures::stream::iter(vec![record_batch]);

    s.map_err(ArrowError::from)
}

impl FileOpener for IndexedFASTAOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        let config = Arc::clone(&self.config);
        let schema = Arc::clone(&self.config.file_schema);
        let file_compression_type = self.file_compression_type;

        Ok(Box::pin(async move {
            let fai_file_range = file_meta
                .extensions
                .as_ref()
                .and_then(|ext| ext.downcast_ref::<FAIFileRange>());

            match fai_file_range {
                Some(fai_file_range) => {
                    let get_options = GetOptions {
                        range: Some(GetRange::Bounded(std::ops::Range {
                            start: fai_file_range.start as usize,
                            end: fai_file_range.end as usize,
                        })),
                        ..Default::default()
                    };

                    let get_result = config
                        .object_store
                        .get_opts(file_meta.location(), get_options)
                        .await?;

                    let get_stream =
                        Box::pin(get_result.into_stream().map_err(DataFusionError::from));

                    if file_compression_type
                        != file_compression_type::FileCompressionType::UNCOMPRESSED
                    {
                        return Err(DataFusionError::Execution(
                            "Indexed FASTA from remote storage only supports uncompressed files."
                                .to_string(),
                        ));
                    }

                    let bytes: Vec<Bytes> = file_compression_type
                        .convert_stream(get_stream)?
                        .collect::<Vec<_>>()
                        .await
                        .into_iter()
                        .collect::<Result<_, _>>()?;

                    let sequence = bytes.into_iter().flatten().collect::<Vec<u8>>();
                    let record_batch =
                        record_batch_stream(&fai_file_range.region_name, &sequence, schema);

                    return Ok(record_batch.boxed());
                }
                None => {
                    let region_extension = file_meta
                        .extensions
                        .as_ref()
                        .and_then(|ext| ext.downcast_ref::<RegionObjectStoreExtension>())
                        .ok_or(DataFusionError::Execution(
                            "Region extension not found".to_string(),
                        ))?;

                    let get_result = config.object_store.get(file_meta.location()).await?;

                    match get_result.payload {
                        GetResultPayload::File(_, path) => {
                            let mut reader = noodles::fasta::indexed_reader::Builder::default()
                                .build_from_path(path)?;

                            let record = reader.query(&region_extension.region)?;

                            let sequence = record.sequence();

                            let record_batch = record_batch_stream(
                                &region_extension.region_name(),
                                sequence.as_ref(),
                                schema,
                            );

                            Ok(record_batch.boxed())
                        }
                        _ => Err(DataFusionError::Execution(
                            "Direct region access only supported on local files.".to_string(),
                        )),
                    }
                }
            }
        }))
    }
}
