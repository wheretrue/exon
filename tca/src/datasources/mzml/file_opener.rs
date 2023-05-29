use std::sync::Arc;

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
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
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let mzml_config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::{
        datasource::file_format::file_type::FileCompressionType,
        physical_plan::file_format::{FileMeta, FileOpener},
    };
    use futures::StreamExt;

    use crate::{
        datasources::mzml::{MzMLConfig, MzMLOpener},
        tests::test_listing_table_dir,
    };

    #[tokio::test]
    async fn test_opener() {
        let config = Arc::new(MzMLConfig::default());

        let path = test_listing_table_dir("mzml", "test.mzml");
        let object_meta = config.object_store.head(&path).await.unwrap();

        let opener = MzMLOpener::new(config, FileCompressionType::UNCOMPRESSED);

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
