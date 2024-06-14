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

use datafusion::datasource::listing::PartitionedFile;
use noodles::{
    core::Region,
    csi::{binning_index::index::reference_sequence::bin::Chunk, BinningIndex},
};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

use datafusion::error::Result;

pub enum IndexedBGZFFile {
    Vcf,
    Bam,
    Gff,
}

impl IndexedBGZFFile {
    pub async fn get_byte_range_for_file(
        &self,
        object_store: Arc<dyn ObjectStore>,
        object_meta: &ObjectMeta,
        region: &Region,
    ) -> Result<Vec<Chunk>> {
        get_byte_range_for_file(object_store, object_meta, region, self).await
    }

    pub fn index_file_extension(&self) -> &str {
        match self {
            Self::Vcf | Self::Gff => ".tbi",
            Self::Bam => ".bai",
        }
    }
}

/// For a given file, get the list of byte ranges that contain the data for the given region.
pub async fn get_byte_range_for_file(
    object_store: Arc<dyn ObjectStore>,
    object_meta: &ObjectMeta,
    region: &Region,
    indexed_file: &IndexedBGZFFile,
) -> Result<Vec<Chunk>> {
    let path = format!(
        "{}{}",
        object_meta.location,
        indexed_file.index_file_extension()
    );
    let path = Path::from(path);

    let index_bytes = object_store.get(&path).await?.bytes().await?;
    let cursor = std::io::Cursor::new(index_bytes);

    let chunks = match indexed_file {
        IndexedBGZFFile::Vcf | IndexedBGZFFile::Gff => {
            let index = noodles::tabix::Reader::new(cursor).read_index()?;

            let header = index.header().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing tabix header")
            })?;

            let region_name = std::str::from_utf8(region.name()).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid region name: {}", e),
                )
            })?;
            let id = header.reference_sequence_names().get_index_of(region_name);

            match id {
                Some(id) => {
                    let chunks = index.query(id, region.interval())?;

                    Ok(chunks)
                }
                None => Ok(vec![]),
            }
        }
        IndexedBGZFFile::Bam => {
            let stream = object_store.get(&object_meta.location).await?.into_stream();
            let reader = StreamReader::new(stream);
            let mut bam_reader = noodles::bam::AsyncReader::new(reader);

            let header = bam_reader.read_header().await?;

            let mut index_reader = noodles::bam::bai::Reader::new(cursor);
            let index = index_reader.read_index()?;

            let id = header.reference_sequences().get_index_of(region.name());

            match id {
                Some(id) => {
                    let chunks = index.query(id, region.interval())?;

                    Ok(chunks)
                }
                None => Ok(vec![]),
            }
        }
    };

    chunks
}

pub(crate) struct BGZFIndexedOffsets {
    pub start: noodles::bgzf::VirtualPosition,
    pub end: noodles::bgzf::VirtualPosition,
}

impl From<Chunk> for BGZFIndexedOffsets {
    fn from(chunk: Chunk) -> Self {
        Self {
            start: chunk.start(),
            end: chunk.end(),
        }
    }
}

/// Augment a partitioned file with the byte ranges that need to be read for a given region
pub(crate) async fn augment_partitioned_file_with_byte_range(
    object_store: Arc<dyn ObjectStore>,
    partitioned_file: &PartitionedFile,
    region: &Region,
    indexed_file: &IndexedBGZFFile,
) -> Result<Vec<PartitionedFile>> {
    let mut new_partition_files = vec![];

    let byte_ranges = indexed_file
        .get_byte_range_for_file(object_store.clone(), &partitioned_file.object_meta, region)
        .await?;

    for byte_range in byte_ranges {
        let index_offsets = BGZFIndexedOffsets::from(byte_range);

        let mut new_partition_file = partitioned_file.clone();
        new_partition_file.extensions = Some(Arc::new(index_offsets));

        new_partition_files.push(new_partition_file);
    }

    Ok(new_partition_files)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use exon_test::test_listing_table_dir;
    use noodles::bgzf::VirtualPosition;
    use object_store::{local::LocalFileSystem, ObjectStore};

    use crate::datasources::indexed_file::indexed_bgzf_file::IndexedBGZFFile;

    #[tokio::test]
    async fn test_byte_range_calculation() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_listing_table_dir("bigger-index", "test.vcf.gz");
        let object_store = Arc::new(LocalFileSystem::new());

        let object_meta = object_store.head(&path).await?;

        let region = "chr1:1-3388930".parse()?;

        let chunks = IndexedBGZFFile::Vcf
            .get_byte_range_for_file(object_store, &object_meta, &region)
            .await?;

        assert_eq!(chunks.len(), 1);

        let chunk = chunks[0];
        assert_eq!(chunk.start(), VirtualPosition::from(621346816));
        assert_eq!(chunk.end(), VirtualPosition::from(3014113427456));

        Ok(())
    }
}
