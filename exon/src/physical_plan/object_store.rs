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

use bytes::Bytes;
use datafusion::datasource::physical_plan::FileMeta;
use object_store::ObjectStore;

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
    let byte_region = object_store
        .get_range(
            file_meta.location(),
            Range {
                start: file_meta.range.clone().map(|r| r.start).unwrap_or(0) as usize,
                end: file_meta.range.clone().map(|r| r.end).unwrap_or(0) as usize,
            },
        )
        .await?;

    Ok(byte_region)
}
