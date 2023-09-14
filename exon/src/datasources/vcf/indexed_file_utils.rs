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

use noodles::{core::Region, csi::index::reference_sequence::bin::Chunk};
use object_store::{path::Path, ObjectMeta, ObjectStore};

/// For a given file, get the list of byte ranges that contain the data for the given region.
pub async fn get_byte_range_for_file(
    object_store: Arc<dyn ObjectStore>,
    object_meta: &ObjectMeta,
    region: &Region,
) -> std::io::Result<Vec<Chunk>> {
    let tbi_path = object_meta.location.clone().to_string() + ".tbi";
    let tbi_path = Path::from(tbi_path);

    let index_bytes = object_store.get(&tbi_path).await?.bytes().await?;

    let cursor = std::io::Cursor::new(index_bytes);
    let index = noodles::tabix::Reader::new(cursor).read_index()?;

    match resolve_region(&index, region)? {
        Some(id) => {
            let chunks = index.query(id, region.interval())?;

            Ok(chunks)
        }
        None => Ok(vec![]),
    }
}

/// Given a region, use its name to resolve the reference sequence index.
fn resolve_region(index: &noodles::csi::Index, region: &Region) -> std::io::Result<Option<usize>> {
    let header = index.header().ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "missing tabix header")
    })?;

    let id = header
        .reference_sequence_names()
        .get_index_of(region.name());

    Ok(id)
}
