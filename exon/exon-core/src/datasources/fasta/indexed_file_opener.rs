use std::{ops::Range, sync::Arc};

use datafusion::{datasource::physical_plan::FileOpener, error::DataFusionError};
use exon_fasta::FASTAConfig;
use noodles::core::Region;
use object_store::{GetOptions, GetRange};

use crate::datasources::indexed_file_utils::IndexOffsets;

#[derive(Debug)]
pub struct IndexedFASTAOpener {
    /// The configuration for the opener.
    config: Arc<FASTAConfig>,

    /// The region to use for filtering.
    region: Arc<Region>,
}

impl IndexedFASTAOpener {
    pub fn new(config: Arc<FASTAConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexedFASTAOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> datafusion::error::Result<datafusion::datasource::physical_plan::FileOpenFuture> {
        let config = self.config.clone();

        Ok(Box::pin(async move {
            // If we have index offsets it means we know the file is bzipped so used the indexed offesets
            if let Some(index_offsets) = file_meta
                .extensions
                .as_ref()
                .and_then(|ext| ext.downcast_ref::<IndexOffsets>())
            {
                todo!("Handle indexed file")
            } else {
                todo!("Handle non-indexed file")
            }
        }))
    }
}
