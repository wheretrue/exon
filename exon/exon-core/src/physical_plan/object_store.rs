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

use arrow::{
    array::{Array, ArrayRef, AsArray, StringBuilder},
    compute::{and, cast, prep_null_mask_filter},
    datatypes::{DataType, Field, Fields, Schema},
    ipc::List,
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::{
    common::{DFField, DFSchema},
    datasource::{listing::FileRange, physical_plan::FileMeta},
    execution::context::SessionState,
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    prelude::Expr,
    scalar::ScalarValue,
};
use object_store::{path::Path, ObjectMeta, ObjectStore};

use datafusion::{
    datasource::listing::{ListingTableUrl, PartitionedFile},
    error::{DataFusionError, Result},
};
use futures::{
    stream::{BoxStream, FuturesUnordered},
    StreamExt, TryStreamExt,
};

const CONCURRENCY_LIMIT: usize = 100;

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
            let partition_values = parse_partition_key_values(path, table_partition_cols).unwrap();

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
        let split = part.as_ref().split('=').collect::<Vec<&str>>();

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

pub(crate) async fn list_all_files<'a>(
    path: &'a ListingTableUrl,
    ctx: &'a SessionState,
    store: &'a dyn ObjectStore,
    file_extension: &'a str,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    // If the prefix is a file, use a head request, otherwise list
    let is_dir = path.as_str().ends_with('/');
    let list = match is_dir {
        true => futures::stream::once(store.list(Some(&path.prefix())))
            .try_flatten()
            .boxed(),
        false => futures::stream::once(store.head(&path.prefix())).boxed(),
    };
    Ok(list
        .try_filter(move |meta| {
            let path = &meta.location;
            let extension_match = path.as_ref().ends_with(file_extension);
            // let glob_match = path.scontains(path); // TODO Fix this
            futures::future::ready(extension_match)
        })
        .map_err(DataFusionError::ObjectStore)
        .boxed())
}

/// Discover the partitions on the given path and prune out files
/// that belong to irrelevant partitions using `filters` expressions.
/// `filters` might contain expressions that can be resolved only at the
/// file level (e.g. Parquet row group pruning).
pub async fn pruned_partition_list<'a>(
    ctx: &'a SessionState,
    store: &'a dyn ObjectStore,
    table_path: &'a ListingTableUrl,
    filters: &'a [Expr],
    file_extension: &'a str,
    partition_cols: &'a [(String, DataType)],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    if partition_cols.is_empty() {
        let files = list_all_files(table_path, ctx, store, file_extension)
            .await?
            .map_ok(|o| o.into());

        return Ok(Box::pin(files));
    }

    let partitions = list_partitions(store, table_path, partition_cols.len()).await?;
    let pruned = prune_partitions(table_path, partitions, filters, partition_cols).await?;

    let stream = futures::stream::iter(pruned)
        .map(move |partition: Partition| async move {
            let cols = partition_cols.iter().map(|x| x.0.as_str());
            let parsed = parse_partitions_for_path(table_path, &partition.path, cols);

            let partition_values = parsed
                .into_iter()
                .flatten()
                .zip(partition_cols)
                .map(|(parsed, (_, datatype))| {
                    ScalarValue::try_from_string(parsed.to_string(), datatype)
                })
                .collect::<Result<Vec<_>>>()?;

            let files = match partition.files {
                Some(files) => files,
                None => {
                    let s = store.clone().list(Some(&partition.path)).await?;
                    s.try_collect().await?
                }
            };

            let files = files.into_iter().filter(move |o| {
                let extension_match = o.location.as_ref().ends_with(file_extension);
                let glob_match = table_path.contains(&o.location);
                extension_match && glob_match
            });

            let stream = futures::stream::iter(files.map(move |object_meta| {
                Ok(PartitionedFile {
                    object_meta,
                    partition_values: partition_values.clone(),
                    range: None,
                    extensions: None,
                })
            }));

            Ok::<_, DataFusionError>(stream)
        })
        .buffer_unordered(CONCURRENCY_LIMIT)
        .try_flatten()
        .boxed();
    Ok(stream)
}

struct Partition {
    /// The path to the partition, including the table prefix
    path: Path,
    /// How many path segments below the table prefix `path` contains
    /// or equivalently the number of partition values in `path`
    depth: usize,
    /// The files contained as direct children of this `Partition` if known
    files: Option<Vec<ObjectMeta>>,
}

impl Partition {
    /// List the direct children of this partition updating `self.files` with
    /// any child files, and returning a list of child "directories"
    async fn list(mut self, store: &dyn ObjectStore) -> Result<(Self, Vec<Path>)> {
        let prefix = Some(&self.path).filter(|p| !p.as_ref().is_empty());
        let result = store.list_with_delimiter(prefix).await?;
        self.files = Some(result.objects);
        Ok((self, result.common_prefixes))
    }
}

async fn list_partitions(
    store: &dyn ObjectStore,
    table_path: &ListingTableUrl,
    max_depth: usize,
) -> Result<Vec<Partition>> {
    let partition = Partition {
        path: table_path.prefix().clone(),
        depth: 0,
        files: None,
    };

    let mut out = Vec::with_capacity(64);

    let mut pending = vec![];
    let mut futures = FuturesUnordered::new();
    futures.push(partition.list(store));

    while let Some((partition, paths)) = futures.next().await.transpose()? {
        // If pending contains a future it implies prior to this iteration
        // `futures.len == CONCURRENCY_LIMIT`. We can therefore add a single
        // future from `pending` to the working set
        if let Some(next) = pending.pop() {
            futures.push(next)
        }

        let depth = partition.depth;
        out.push(partition);
        for path in paths {
            let child = Partition {
                path,
                depth: depth + 1,
                files: None,
            };
            match depth < max_depth {
                true => match futures.len() < CONCURRENCY_LIMIT {
                    true => futures.push(child.list(store)),
                    false => pending.push(child.list(store)),
                },
                false => out.push(child),
            }
        }
    }
    Ok(out)
}

async fn prune_partitions(
    table_path: &ListingTableUrl,
    partitions: Vec<Partition>,
    filters: &[Expr],
    partition_cols: &[(String, DataType)],
) -> Result<Vec<Partition>> {
    if filters.is_empty() {
        return Ok(partitions);
    }

    let mut builders: Vec<_> = (0..partition_cols.len())
        .map(|_| StringBuilder::with_capacity(partitions.len(), partitions.len() * 10))
        .collect();

    for partition in &partitions {
        let cols = partition_cols.iter().map(|x| x.0.as_str());
        let parsed =
            parse_partitions_for_path(table_path, &partition.path, cols).unwrap_or_default();

        let mut builders = builders.iter_mut();
        for (p, b) in parsed.iter().zip(&mut builders) {
            b.append_value(p);
        }
        builders.for_each(|b| b.append_null());
    }

    let arrays = partition_cols
        .iter()
        .zip(builders)
        .map(|((_, d), mut builder)| {
            let array = builder.finish();
            cast(&array, d)
        })
        .collect::<Result<_, _>>()?;

    let fields: Fields = partition_cols
        .iter()
        .map(|(n, d)| Field::new(n, d.clone(), true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    let df_schema = DFSchema::new_with_metadata(
        partition_cols
            .iter()
            .map(|(n, d)| DFField::new_unqualified(n, d.clone(), true))
            .collect(),
        Default::default(),
    )?;

    let batch = RecordBatch::try_new(schema.clone(), arrays)?;

    // TODO: Plumb this down
    let props = ExecutionProps::new();

    // Applies `filter` to `batch` returning `None` on error
    let do_filter = |filter| -> Option<ArrayRef> {
        let expr = create_physical_expr(filter, &df_schema, &schema, &props).ok()?;
        Some(expr.evaluate(&batch).ok()?.into_array(partitions.len()))
    };

    //.Compute the conjunction of the filters, ignoring errors
    let mask = filters
        .iter()
        .fold(None, |acc, filter| match (acc, do_filter(filter)) {
            (Some(a), Some(b)) => Some(and(&a, b.as_boolean()).unwrap_or(a)),
            (None, Some(r)) => Some(r.as_boolean().clone()),
            (r, None) => r,
        });

    let mask = match mask {
        Some(mask) => mask,
        None => return Ok(partitions),
    };

    // Don't retain partitions that evaluated to null
    let prepared = match mask.null_count() {
        0 => mask,
        _ => prep_null_mask_filter(&mask),
    };

    // Sanity check
    assert_eq!(prepared.len(), partitions.len());

    let filtered = partitions
        .into_iter()
        .zip(prepared.values())
        .filter_map(|(p, f)| f.then_some(p))
        .collect();

    Ok(filtered)
}

fn parse_partitions_for_path<'a, I>(
    table_path: &ListingTableUrl,
    file_path: &'a Path,
    table_partition_cols: I,
) -> Option<Vec<&'a str>>
where
    I: IntoIterator<Item = &'a str>,
{
    // let subpath = table_path.strip_prefix(file_path)?;
    let subpath = strip_prefix(table_path, file_path)?;

    let mut part_values = vec![];
    for (part, pn) in subpath.zip(table_partition_cols) {
        match part.split_once('=') {
            Some((name, val)) if name == pn => part_values.push(val),
            _ => {
                return None;
            }
        }
    }
    Some(part_values)
}

fn strip_prefix<'a, 'b: 'a>(
    table_path: &'a ListingTableUrl,
    path: &'b Path,
) -> Option<impl Iterator<Item = &'b str> + 'a> {
    use object_store::path::DELIMITER;
    let mut stripped = path.as_ref().strip_prefix(table_path.prefix().as_ref())?;
    if !stripped.is_empty() && !table_path.prefix().as_ref().is_empty() {
        stripped = stripped.strip_prefix(DELIMITER)?;
    }
    Some(stripped.split_terminator(DELIMITER))
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
