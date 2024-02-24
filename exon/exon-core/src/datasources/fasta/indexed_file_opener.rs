use std::sync::Arc;

use arrow::{
    array::{RecordBatch, StringArray},
    datatypes::Schema,
    error::ArrowError,
};
use bytes::Bytes;
use datafusion::{
    datasource::{
        file_format::file_compression_type::FileCompressionType, physical_plan::FileOpener,
    },
    error::DataFusionError,
};
use exon_fasta::FASTAConfig;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{GetOptions, GetRange};

use crate::datasources::indexed_file::fai::FAIFileRange;

#[derive(Debug)]
pub struct IndexedFASTAOpener {
    /// The configuration for the opener.
    config: Arc<FASTAConfig>,
}

impl IndexedFASTAOpener {
    pub fn new(config: Arc<FASTAConfig>) -> Self {
        Self { config }
    }
}

fn record_batch_stream(
    sequence_name: &str,
    sequence_literal: &[u8],
    record_schema: Arc<Schema>,
) -> impl Stream<Item = arrow::error::Result<RecordBatch>> {
    // Create a single record batch with the sequence literal.
    let record_batch = RecordBatch::try_new(
        record_schema.clone(),
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
        let config = self.config.clone();
        let schema = self.config.file_schema.clone();

        Ok(Box::pin(async move {
            let fai_file_range = file_meta
                .extensions
                .as_ref()
                .and_then(|ext| ext.downcast_ref::<FAIFileRange>())
                .ok_or(DataFusionError::Internal(
                    "Expected FAIFileRange extension".to_string(),
                ))?;

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

            let get_stream = Box::pin(get_result.into_stream().map_err(DataFusionError::from));
            let bytes: Vec<Bytes> = FileCompressionType::UNCOMPRESSED
                .convert_stream(get_stream)?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<_, _>>()?;

            let sequence = bytes.into_iter().flatten().collect::<Vec<u8>>();
            let record_batch = record_batch_stream(&fai_file_range.region_name, &sequence, schema);

            return Ok(record_batch.boxed());
        }))
    }
}
