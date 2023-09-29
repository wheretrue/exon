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

use std::{io, sync::Arc};

use datafusion::{
    datasource::{
        listing::FileRange,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener},
    },
    error::DataFusionError,
};
use futures::{StreamExt, TryStreamExt};
use noodles::{
    bam::lazy::record::Data,
    bgzf::{self, VirtualPosition},
    core::Region,
    sam::Header,
};
use object_store::GetResultPayload;
use tokio_util::io::StreamReader;

use crate::streaming_bgzf::AsyncBGZFReader;

use super::{
    batch_reader::{BatchReader, StreamRecordBatchAdapter},
    config::BAMConfig,
};

/// Implements a datafusion `FileOpener` for BAM files.
pub struct IndexedBAMOpener {
    /// The base configuration for the file scan.
    config: Arc<BAMConfig>,

    /// An optional region to filter on.
    region: Arc<Region>,
}

impl IndexedBAMOpener {
    /// Create a new BAM file opener.
    pub fn new(config: Arc<BAMConfig>, region: Arc<Region>) -> Self {
        Self { config, region }
    }
}

impl FileOpener for IndexedBAMOpener {
    fn open(&self, file_meta: FileMeta) -> datafusion::error::Result<FileOpenFuture> {
        let config = self.config.clone();
        let region = self.region.clone();

        todo!();

        // Ok(Box::pin(async move {
        //     let get_request = config.object_store.get(file_meta.location()).await?;
        //     let get_stream = get_request.into_stream();

        //     let stream_reader = Box::pin(get_stream.map_err(DataFusionError::from));
        //     let stream_reader = StreamReader::new(stream_reader);

        //     let mut first_bam_reader = noodles::bam::AsyncReader::new(stream_reader);

        //     let bam_header: Header =
        //         first_bam_reader.read_header().await?.parse().map_err(|_| {
        //             DataFusionError::Execution("Error parsing BAM header".to_string())
        //         })?;

        //     first_bam_reader.read_reference_sequences().await?;

        //     let header_offset = first_bam_reader.virtual_position();

        //     let bgzf_reader = match file_meta.range {
        //         Some(FileRange { start, end }) => {
        //             let vp_start = VirtualPosition::from(start as u64);
        //             let vp_end = VirtualPosition::from(end as u64);

        //             if vp_end.compressed() == 0 {
        //                 let stream = config
        //                     .object_store
        //                     .get(file_meta.location())
        //                     .await?
        //                     .into_stream()
        //                     .map_err(DataFusionError::from);

        //                 let stream_reader = StreamReader::new(Box::pin(stream));

        //                 let mut async_reader = AsyncBGZFReader::from_reader(stream_reader);
        //                 async_reader.scan_to_virtual_position(header_offset).await?;

        //                 async_reader
        //             }
        //         }
        //         None => {
        //             todo!()
        //         }
        //     };

        //     // let bgzf_reader = AsyncBGZFReader::new(bgzf_reader);
        //     let bgzf_reader = bgzf_reader.into_inner();

        //     let bam_reader = noodles::bam::AsyncReader::from(bgzf_reader);

        //     let lazy_records = bam_reader.lazy_records().await;

        // match get_request.payload {
        //     GetResultPayload::File(_file, path) => {
        //         let mut reader =
        //             noodles::bam::indexed_reader::Builder::default().build_from_path(path)?;

        //         let header = reader.read_header()?;

        //         let query = reader
        //             .query(&header, &region)?
        //             .map(Ok)
        //             .collect::<io::Result<Vec<_>>>()?;

        //         let record_stream = futures::stream::iter(query).boxed();

        //         let batch_adapter =
        //             StreamRecordBatchAdapter::new(record_stream, header, config.clone());

        //         let batch_stream = batch_adapter.into_stream();

        //         Ok(batch_stream.boxed())
        //     }
        //     GetResultPayload::Stream(s) => {
        //         let stream_reader = Box::pin(s.map_err(DataFusionError::from));

        //         let stream_reader = StreamReader::new(stream_reader);
        //         let batch_reader = BatchReader::new(stream_reader, config).await?;

        //         let batch_stream = batch_reader.into_stream();

        //         Ok(batch_stream.boxed())
        //     }
        // }
        // }))
    }
}
