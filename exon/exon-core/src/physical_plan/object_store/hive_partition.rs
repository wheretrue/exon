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

// This code is largely adapted from DataFusion's hive handling accessible in the crate

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, AsArray, StringBuilder},
    compute::{and, cast, prep_null_mask_filter},
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};

use datafusion::{
    common::DFSchema,
    datasource::listing::{ListingTableUrl, PartitionedFile},
    error::{DataFusionError, Result},
    execution::context::SessionState,
    physical_expr::{create_physical_expr, execution_props::ExecutionProps},
    prelude::Expr,
    scalar::ScalarValue,
};
use futures::{
    stream::{BoxStream, FuturesUnordered},
    StreamExt, TryStreamExt,
};
use object_store::{path::Path, ObjectMeta, ObjectStore};

const CONCURRENCY_LIMIT: usize = 100;

pub(crate) async fn list_all_files<'a>(
    path: &'a ListingTableUrl,
    _ctx: &'a SessionState,
    store: &'a dyn ObjectStore,
    file_extension: &'a str,
) -> Result<BoxStream<'a, Result<ObjectMeta>>> {
    // If the prefix is a file, use a head request, otherwise list
    let is_dir = path.as_str().ends_with('/');
    let list = match is_dir {
        true => store.list(Some(path.prefix())),
        false => futures::stream::once(store.head(path.prefix())).boxed(),
    };
    Ok(list
        .try_filter(move |meta| {
            let path = &meta.location;
            let extension_match = path
                .as_ref()
                .to_lowercase()
                .ends_with(file_extension.to_lowercase().as_str());

            // let glob_match = path.contains(path); // TODO Fix this
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
    partition_cols: &'a [Field],
) -> Result<BoxStream<'a, Result<PartitionedFile>>> {
    tracing::info!(
        "pruned_partition_list: table_path: {:?}, partition_cols: {:?}, file_extension: {:?}",
        table_path,
        partition_cols,
        file_extension
    );

    if partition_cols.is_empty() {
        let files = list_all_files(table_path, ctx, store, file_extension)
            .await?
            .map_ok(|o| o.into());

        tracing::trace!("pruned_partition_list: no partition columns, returning all files");

        return Ok(Box::pin(files));
    }

    let partitions = list_partitions(store, table_path, partition_cols.len()).await?;
    let pruned = prune_partitions(table_path, partitions, filters, partition_cols).await?;

    tracing::info!(
        "pruned_partition_list: got n partitions {:?} for table_path {:?}",
        pruned.len(),
        table_path
    );

    let stream = futures::stream::iter(pruned)
        .map(move |partition: Partition| async move {
            let cols = partition_cols.iter().map(|x| x.name().as_str());
            let parsed = parse_partitions_for_path(table_path, &partition.path, cols);

            let partition_values = parsed
                .into_iter()
                .flatten()
                .zip(partition_cols)
                .map(|(parsed, field)| {
                    ScalarValue::try_from_string(parsed.to_string(), field.data_type())
                })
                .collect::<Result<Vec<_>>>()?;

            let files = match partition.files {
                Some(files) => files,
                None => {
                    let s = store.list(Some(&partition.path));
                    s.try_collect().await?
                }
            };

            let files = files.into_iter().filter(move |o| {
                let extension_match = o.location.as_ref().to_lowercase().ends_with(file_extension);
                let glob_match = table_path.contains(&o.location, false);

                tracing::info!(
                    "pruned_partition_list: extension_match: {:?}, glob_match: {:?} for {:?}",
                    extension_match,
                    glob_match,
                    o.location
                );
                extension_match && glob_match
            });

            let stream = futures::stream::iter(files.map(move |object_meta| {
                Ok(PartitionedFile {
                    object_meta,
                    partition_values: partition_values.clone(),
                    range: None,
                    extensions: None,
                    statistics: None,
                })
            }));

            Ok::<_, DataFusionError>(stream)
        })
        .buffer_unordered(CONCURRENCY_LIMIT)
        .try_flatten()
        .boxed();
    Ok(stream)
}

#[derive(Debug)]
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
    partition_cols: &[Field],
) -> Result<Vec<Partition>> {
    if filters.is_empty() {
        return Ok(partitions);
    }

    let mut builders: Vec<_> = (0..partition_cols.len())
        .map(|_| StringBuilder::with_capacity(partitions.len(), partitions.len() * 10))
        .collect();

    for partition in &partitions {
        let cols = partition_cols.iter().map(|x| x.name().as_str());
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
        .map(|(field, mut builder)| {
            let array = builder.finish();
            cast(&array, field.data_type()).map_err(|e| DataFusionError::ArrowError(e, None))
        })
        .collect::<Result<_, _>>()?;

    // let fields: Fields = partition_cols.collect();
    let schema = Arc::new(Schema::new(partition_cols.to_vec()));

    let df_schema = DFSchema::from_unqualified_fields(
        partition_cols
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), f.is_nullable())) // TODO: use qualified name, remove clone
            .collect(),
        Default::default(),
    )?;

    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;

    let props = ExecutionProps::new();

    // Applies `filter` to `batch` returning `None` on error
    let do_filter = |filter| -> Option<ArrayRef> {
        let expr = create_physical_expr(filter, &df_schema, &props).ok()?;

        let eval = expr.evaluate(&batch).ok()?;
        eval.into_array(partitions.len()).ok()
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
