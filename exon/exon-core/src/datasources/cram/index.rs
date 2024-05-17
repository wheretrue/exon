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

use crate::error::Result as ExonResult;
use datafusion::datasource::listing::PartitionedFile;
use itertools::Itertools;
use noodles::core::Region;
use object_store::{path::Path, ObjectStore};

pub(crate) struct CRAMIndexData {
    pub header: noodles::sam::Header,
    pub records: Vec<noodles::cram::crai::Record>,
    pub offset: u64,
}

pub(crate) async fn augment_file_with_crai_record_chunks(
    object_store: Arc<dyn ObjectStore>,
    header: &noodles::sam::Header,
    partitioned_file: &PartitionedFile,
    region: &Region,
) -> ExonResult<Vec<PartitionedFile>> {
    let path = format!("{}.crai", partitioned_file.object_meta.location);
    let path = Path::from(path);

    let index_bytes = object_store.get(&path).await?.bytes().await?;
    let cursor = std::io::Cursor::new(index_bytes);

    let index_records = noodles::cram::crai::Reader::new(cursor).read_index()?;

    let chunks = index_records
        .iter()
        .filter(|r| {
            if let Some(seq_id) = header.reference_sequences().get_index_of(region.name()) {
                if let Some(r_seq_id) = r.reference_sequence_id() {
                    if seq_id != r_seq_id {
                        return false;
                    }

                    if let Some(start) = r.alignment_start() {
                        region.interval().contains(start)
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            }
        })
        .sorted_by(|a, b| a.offset().cmp(&b.offset()))
        .chunk_by(|a| a.offset())
        .into_iter()
        .map(|(offset, records)| {
            let mut pf = partitioned_file.clone();

            let owned_records = records.cloned().collect::<Vec<_>>();

            let index_data = CRAMIndexData {
                header: header.clone(),
                offset,
                records: owned_records,
            };

            pf.extensions = Some(Arc::new(index_data));

            pf
        })
        .collect::<Vec<PartitionedFile>>();

    Ok(chunks)
}
