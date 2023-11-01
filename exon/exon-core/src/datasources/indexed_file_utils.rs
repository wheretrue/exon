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

use std::sync::Arc;

use datafusion::{
    datasource::listing::{FileRange, ListingTableUrl, PartitionedFile},
    error::DataFusionError,
    error::Result,
};
use futures::TryStreamExt;
use noodles::{core::Region, csi::index::reference_sequence::bin::Chunk};
use object_store::{path::Path, ObjectMeta, ObjectStore};
use tokio_util::io::StreamReader;

pub enum IndexedFile {
    Vcf,
    Bam,
}

impl IndexedFile {
    pub async fn get_byte_range_for_file(
        &self,
        object_store: Arc<dyn ObjectStore>,
        object_meta: &ObjectMeta,
        region: &Region,
    ) -> std::io::Result<Vec<Chunk>> {
        get_byte_range_for_file(object_store, object_meta, region, self).await
    }

    pub async fn list_files_for_scan(
        &self,
        table_paths: Vec<ListingTableUrl>,
        store: Arc<dyn ObjectStore>,
        regions: &[Region],
    ) -> Result<Vec<Vec<PartitionedFile>>> {
        list_files_for_scan(table_paths, store, regions, self).await
    }

    pub fn index_file_extension(&self) -> &str {
        match self {
            Self::Vcf => ".tbi",
            Self::Bam => ".bai",
        }
    }

    pub fn file_extension(&self) -> &str {
        match self {
            Self::Vcf => ".vcf.gz",
            Self::Bam => ".bam",
        }
    }
}

/// For a given file, get the list of byte ranges that contain the data for the given region.
pub async fn get_byte_range_for_file(
    object_store: Arc<dyn ObjectStore>,
    object_meta: &ObjectMeta,
    region: &Region,
    indexed_file: &IndexedFile,
) -> std::io::Result<Vec<Chunk>> {
    let path = object_meta.location.clone().to_string() + indexed_file.index_file_extension();
    let path = Path::from(path);

    let index_bytes = object_store.get(&path).await?.bytes().await?;

    let cursor = std::io::Cursor::new(index_bytes);

    let chunks = match indexed_file {
        IndexedFile::Vcf => {
            let index = noodles::tabix::Reader::new(cursor).read_index()?;

            let header = index.header().ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing tabix header")
            })?;

            let id = header
                .reference_sequence_names()
                .get_index_of(region.name());

            match id {
                Some(id) => {
                    let chunks = index.query(id, region.interval())?;

                    Ok(chunks)
                }
                None => Ok(vec![]),
            }
        }
        IndexedFile::Bam => {
            let stream = object_store.get(&object_meta.location).await?.into_stream();
            let reader = StreamReader::new(stream);
            let mut bam_reader = noodles::bam::AsyncReader::new(reader);

            let header = bam_reader.read_header().await?;
            let header: noodles::sam::Header = header.parse().unwrap();

            let mut index_reader = noodles::bam::bai::Reader::new(cursor);
            index_reader.read_header()?;
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

/// List the files that need to be read for a given set of regions
async fn list_files_for_scan(
    table_paths: Vec<ListingTableUrl>,
    store: Arc<dyn ObjectStore>,
    regions: &[Region],
    indexed_file: &IndexedFile,
) -> Result<Vec<Vec<PartitionedFile>>> {
    let mut lists = Vec::new();

    for table_path in &table_paths {
        if table_path.as_str().ends_with('/') {
            let store_list = store.list(Some(table_path.prefix())).await?;

            // iterate over all files in the listing
            let mut result_vec: Vec<PartitionedFile> = vec![];

            store_list
                .try_for_each(|v| {
                    let path = v.location.clone();
                    let extension_match = path.as_ref().ends_with(indexed_file.file_extension());
                    let glob_match = table_path.contains(&path);
                    if extension_match && glob_match {
                        result_vec.push(v.into());
                    }
                    futures::future::ready(Ok(()))
                })
                .await?;

            lists.push(result_vec);
        } else {
            let store_head = match store.head(table_path.prefix()).await {
                Ok(object_meta) => object_meta,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Unable to get path info: {}",
                        e
                    )))
                }
            };

            lists.push(vec![store_head.into()]);
        }
    }

    let mut new_list = vec![];
    for partition_files in lists {
        let mut new_partition_files = vec![];

        for partition_file in partition_files {
            for region in regions.iter() {
                let byte_ranges = match indexed_file
                    .get_byte_range_for_file(store.clone(), &partition_file.object_meta, region)
                    .await
                {
                    Ok(byte_ranges) => byte_ranges,
                    Err(_) => {
                        continue;
                    }
                };

                for byte_range in byte_ranges {
                    let mut new_partition_file = partition_file.clone();

                    let start = u64::from(byte_range.start());
                    let end = u64::from(byte_range.end());

                    new_partition_file.range = Some(FileRange {
                        start: start as i64,
                        end: end as i64,
                    });
                    new_partition_files.push(new_partition_file);
                }
            }
        }

        new_list.push(new_partition_files);
    }

    Ok(new_list)
}

pub(crate) async fn augment_partitioned_file_with_byte_range(
    object_store: Arc<dyn ObjectStore>,
    partitioned_file: &PartitionedFile,
    region: &Region,
    indexed_file: &IndexedFile,
) -> Result<Vec<PartitionedFile>> {
    let mut new_partition_files = vec![];

    let byte_ranges = indexed_file
        .get_byte_range_for_file(object_store.clone(), &partitioned_file.object_meta, region)
        .await?;

    for byte_range in byte_ranges {
        let mut new_partition_file = partitioned_file.clone();

        let start = u64::from(byte_range.start());
        let end = u64::from(byte_range.end());

        new_partition_file.range = Some(FileRange {
            start: start as i64,
            end: end as i64,
        });
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

    use crate::datasources::indexed_file_utils::IndexedFile;

    #[tokio::test]
    async fn test_byte_range_calculation() -> Result<(), Box<dyn std::error::Error>> {
        let path = test_listing_table_dir("bigger-index", "test.vcf.gz");
        let object_store = Arc::new(LocalFileSystem::new());

        let object_meta = object_store.head(&path).await?;

        let region = "chr1:1-3388930".parse()?;

        let chunks = IndexedFile::Vcf
            .get_byte_range_for_file(object_store, &object_meta, &region)
            .await?;

        assert_eq!(chunks.len(), 1);

        let chunk = chunks[0];
        assert_eq!(chunk.start(), VirtualPosition::from(621346816));
        assert_eq!(chunk.end(), VirtualPosition::from(3014113427456));

        Ok(())
    }
}
