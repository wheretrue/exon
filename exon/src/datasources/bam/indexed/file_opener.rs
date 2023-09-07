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

use std::{ops::Range, sync::Arc};

use dashmap::DashMap;
use datafusion::{
    datasource::physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{
    bam::{self, lazy},
    bgzf::{self, VirtualPosition},
    sam::Header,
};
use object_store::{path::Path, GetOptions};
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;

use crate::datasources::bam::{
    lazy_record_stream::{LazyBatchReader, LazyRecordIterator},
    BAMConfig,
};

#[derive(Debug)]
pub struct FileHeader {
    // The header of the BAM file.
    pub header: Arc<Header>,

    // The offset of the first record in the file.
    pub offset: VirtualPosition,
}

pub type HeaderCache = DashMap<Path, FileHeader>;

/// Implements a datafusion `FileOpener` for BAM files.
pub struct IndexedBAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,

    /// A cache of BAM file headers.
    header_cache: Arc<HeaderCache>,
}

impl IndexedBAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>, header_cache: Arc<HeaderCache>) -> Self {
        Self {
            config,
            header_cache,
        }
    }
}

impl FileOpener for IndexedBAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();

        let header_cache = self.header_cache.clone();

        Ok(Box::pin(async move {
            let file_header =
                header_cache
                    .get(&file_meta.location())
                    .ok_or(DataFusionError::Execution(
                        "Failed to get file header".to_string(),
                    ))?;

            let FileMeta {
                object_meta, range, ..
            } = file_meta;

            let range = range.ok_or(DataFusionError::Execution(
                "Failed to get file range".to_string(),
            ))?;

            let mut options = GetOptions::default();
            options.range = Some(Range {
                start: range.start as usize,
                end: range.end as usize,
            });

            eprintln!("Reading range: {:?}", options.range);
            eprintln!("Location: {:?}", object_meta.location);

            let head = config.object_store.head(&object_meta.location).await?;
            eprintln!("Head: {:?}", head);

            let get_range = config
                .object_store
                .get_opts(&object_meta.location, options)
                .await
                .unwrap();

            let stream_reader = StreamReader::new(get_range.into_stream());
            let mut bam_reader = noodles::bam::AsyncReader::new(stream_reader);

            let mut lazy_record = lazy::Record::default();

            loop {
                let i = bam_reader.read_lazy_record(&mut lazy_record).await.unwrap();
                println!("{:?}", lazy_record);
                break;
            }

            // bam_reader.read_header().await?;

            let record_iterator = LazyRecordIterator::new(bam_reader).into_stream().boxed();

            let header = file_header.header.clone();
            let batch_stream = LazyBatchReader::new(record_iterator, header, config)
                .into_stream()
                .boxed();

            Ok(batch_stream)
        }))
    }
}
