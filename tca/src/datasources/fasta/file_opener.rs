use std::sync::Arc;

use datafusion::{
    datasource::file_format::file_type::FileCompressionType, error::DataFusionError,
    physical_plan::file_format::FileOpener,
};
use futures::{StreamExt, TryStreamExt};
use tokio_util::io::StreamReader;

use super::{batch_reader::BatchReader, config::FASTAConfig};

/// Implements a datafusion `FileOpener` for FASTA files.
pub struct FASTAOpener {
    /// The base configuration for the file scan.
    config: Arc<FASTAConfig>,

    /// The compression type of the file.
    file_compression_type: FileCompressionType,
}

impl FASTAOpener {
    pub fn new(config: Arc<FASTAConfig>, file_compression_type: FileCompressionType) -> Self {
        Self {
            config,
            file_compression_type,
        }
    }
}

impl FileOpener for FASTAOpener {
    fn open(
        &self,
        file_meta: datafusion::physical_plan::file_format::FileMeta,
    ) -> datafusion::error::Result<datafusion::physical_plan::file_format::FileOpenFuture> {
        let fasta_config = self.config.clone();
        let file_compression_type = self.file_compression_type.clone();

        Ok(Box::pin(async move {
            let get_result = fasta_config.object_store.get(file_meta.location()).await?;

            let stream_reader = Box::pin(get_result.into_stream().map_err(DataFusionError::from));

            let new_reader = match file_compression_type.convert_stream(stream_reader) {
                Ok(reader) => reader,
                Err(e) => return Err(e),
            };

            let stream_reader = StreamReader::new(new_reader);

            let fasta_batch_reader = BatchReader::new(stream_reader, fasta_config).into_stream();

            Ok(fasta_batch_reader.boxed())
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
        datasources::fasta::{config::FASTAConfig, file_opener::FASTAOpener},
        tests::test_listing_table_dir,
    };

    #[tokio::test]
    async fn test_opener() {
        let config = Arc::new(FASTAConfig::default());

        let path = test_listing_table_dir("fasta", "test.fasta");
        let object_meta = config.object_store.head(&path).await.unwrap();

        let opener = FASTAOpener::new(config, FileCompressionType::UNCOMPRESSED);

        let file_meta = FileMeta::from(object_meta);

        let mut opened_file = opener.open(file_meta).unwrap().await.unwrap();

        let mut n_records = 0;
        while let Some(batch) = opened_file.next().await {
            let batch = batch.unwrap();
            n_records += batch.num_rows();
        }

        assert_eq!(n_records, 2);
    }
}
