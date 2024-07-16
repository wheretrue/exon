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

use std::io::Result;

use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectMeta, ObjectStore};

/// Returns a stream of object store files for a given table path.
pub async fn object_store_files_from_table_path<'a>(
    store: &'a dyn ObjectStore,
    url: &'a url::Url,
    table_prefix: &'a Path,
    file_extension: &'a str,
    glob: Option<glob::Pattern>,
) -> BoxStream<'a, Result<ObjectMeta>> {
    let object_list = if url.as_str().ends_with('/') {
        let list = store.list(Some(table_prefix));
        list
    } else {
        futures::stream::once(store.head(table_prefix)).boxed()
    };

    object_list
        .map_err(Into::into)
        .try_filter(move |meta| {
            let path = &meta.location;
            let extension_match = path.as_ref().ends_with(file_extension);
            let glob_match = match glob {
                Some(ref glob) => glob.matches(path.as_ref()),
                None => true,
            };
            futures::future::ready(extension_match && glob_match)
        })
        .boxed()
}
