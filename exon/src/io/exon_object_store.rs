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
    datasource::listing::{ListingTableUrl, PartitionedFile},
    error::{DataFusionError, Result},
};
use futures::TryStreamExt;
use object_store::ObjectStore;

pub async fn list_files_for_scan(
    store: Arc<dyn ObjectStore>,
    listing_table_urls: Vec<ListingTableUrl>,
    file_extension: &str,
) -> Result<Vec<PartitionedFile>> {
    let mut lists: Vec<PartitionedFile> = Vec::new();

    for table_path in &listing_table_urls {
        if table_path.as_str().ends_with('/') {
            // We're working with a directory, so we need to list all files in the directory

            let store_list = store.list(Some(table_path.prefix())).await?;

            store_list
                .try_for_each(|v| {
                    let path = v.location.clone();
                    let extension_match = path.as_ref().to_lowercase().ends_with(file_extension);
                    let glob_match = table_path.contains(&path);
                    if extension_match && glob_match {
                        lists.push(v.into());
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

            lists.push(store_head.into());
        }
    }

    Ok(lists)
}
