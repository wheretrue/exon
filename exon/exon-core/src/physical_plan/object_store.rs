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

use arrow::datatypes::DataType;
use bytes::Bytes;
use datafusion::{
    datasource::{listing::FileRange, physical_plan::FileMeta},
    execution::context::SessionState,
    prelude::Expr,
    scalar::ScalarValue,
};
use object_store::{path::Path, ObjectStore};

use datafusion::{
    datasource::listing::{ListingTableUrl, PartitionedFile},
    error::{DataFusionError, Result},
};
use futures::{stream::BoxStream, TryStreamExt};

/// Get a byte region from an object store.
///
/// # Args
///
/// * `object_store` - The object store to get the byte region from.
/// * `file_meta` - The file meta to get the byte region from.
pub async fn get_byte_region(
    object_store: &Arc<dyn ObjectStore>,
    file_meta: FileMeta,
) -> std::io::Result<Bytes> {
    match file_meta.range {
        Some(FileRange { start, end }) if end > 0 => {
            let byte_region = object_store
                .get_range(
                    file_meta.location(),
                    Range {
                        start: start as usize,
                        end: end as usize,
                    },
                )
                .await?;
            Ok(byte_region)
        }
        Some(_) | None => {
            let byte_region = object_store
                .get(file_meta.location())
                .await?
                .bytes()
                .await?;
            Ok(byte_region)
        }
    }
}

/// List files for a scan
pub async fn list_files_for_scan(
    store: Arc<dyn ObjectStore>,
    listing_table_urls: Vec<ListingTableUrl>,
    file_extension: &str,
    table_partition_cols: &[String],
) -> Result<Vec<PartitionedFile>> {
    let mut lists: Vec<PartitionedFile> = Vec::new();

    for table_path in &listing_table_urls {
        if table_path.as_str().ends_with('/') {
            // We're working with a directory, so we need to list all files in the directory

            let store_list = store.list(Some(table_path.prefix())).await?;

            store_list
                .try_for_each(|v| {
                    let path = v.location.clone();

                    let partition_values =
                        parse_partition_key_values(&path, table_partition_cols).unwrap();

                    let extension_match = path.as_ref().to_lowercase().ends_with(file_extension);
                    let glob_match = table_path.contains(&path);
                    if extension_match && glob_match {
                        let mut pc: PartitionedFile = v.into();
                        pc.partition_values = partition_values
                            .into_iter()
                            .map(|(_, v)| ScalarValue::Utf8(Some(v)))
                            .collect();

                        lists.push(pc);
                    }
                    futures::future::ready(Ok(()))
                })
                .await?;
        } else {
            // We're working with a single file, so we need to get the file info
            let store_head = match store.head(table_path.prefix()).await {
                Ok(object_meta) => object_meta,
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "Unable to get path info: {}",
                        e
                    )))
                }
            };

            let path = table_path.prefix();
            let partition_values = parse_partition_key_values(&path, table_partition_cols).unwrap();

            let mut pc: PartitionedFile = store_head.into();
            pc.partition_values = partition_values
                .into_iter()
                .map(|(_, v)| ScalarValue::Utf8(Some(v)))
                .collect();

            lists.push(pc);
        }
    }

    Ok(lists)
}

fn parse_partition_key_values(
    path: &Path,
    table_partition_cols: &[String],
) -> Result<Vec<(String, String)>> {
    let mut partition_key_values: Vec<(String, String)> = Vec::new();

    let mut col_i = 0;
    let max_i = table_partition_cols.len();
    if max_i == 0 {
        return Ok(partition_key_values);
    }

    for part in path.parts() {
        let split = part.as_ref().split("=").collect::<Vec<&str>>();

        if split.len() != 2 {
            continue;
        }

        let key = split[0];
        let value = split[1];

        if key == table_partition_cols[col_i] {
            partition_key_values.push((key.to_string(), value.to_string()));

            col_i += 1;
        }
    }

    if col_i != max_i {
        return Err(DataFusionError::Execution(format!(
            "Unable to parse partition key values: {}",
            path
        )));
    }

    Ok(partition_key_values)
}

#[cfg(test)]
mod tests {
    use object_store::path::Path;

    use crate::physical_plan::object_store::parse_partition_key_values;

    #[tokio::test]
    async fn test_partition_parse_simple() -> Result<(), Box<dyn std::error::Error>> {
        let path = Path::parse("users/thauck/wheretrue/github.com/wheretrue/exon/exon/exon-core/test-data/datasources/gff-partition/sample=1/test.gff")?;
        let table_partition_cols = vec!["sample".to_string()];

        let partition_values = parse_partition_key_values(&path, &table_partition_cols)?;

        assert_eq!(partition_values.len(), 1);
        assert_eq!(partition_values[0].0, "sample");
        assert_eq!(partition_values[0].1, "1");

        Ok(())
    }
}
