use std::sync::Arc;

use datafusion::{error::DataFusionError, physical_plan::file_format::FileOpener};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::BAMConfig};

/// Implements a datafusion `FileOpener` for BAM files.
pub struct BAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,
}

impl BAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>) -> Self {
        Self { config }
    }
}

impl FileOpener for BAMOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let config = self.config.clone();

        Ok(Box::pin(async move {
            let get_result = config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));

            let stream_reader = StreamReader::new(stream_reader);
            let batch_reader = BatchReader::new(stream_reader, config).await?;

            let batch_stream = batch_reader.into_stream();

            Ok(batch_stream.boxed())
        }))
    }
}
