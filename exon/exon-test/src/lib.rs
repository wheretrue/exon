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

use std::path::PathBuf;

use datafusion::datasource::listing::ListingTableUrl;
use object_store::path::Path;

pub fn test_path(data_type: &str, file_name: &str) -> PathBuf {
    // Get the current working directory
    let cwd = std::env::current_dir().unwrap().join("exon");

    let start_directory = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or(cwd);

    if !start_directory.exists() {
        panic!("start directory does not exist: {:?}", start_directory);
    }

    start_directory
        .join("test-data")
        .join("datasources")
        .join(data_type)
        .join(file_name)
}

/// Get a path to a test listing table directory. A helper function not for main use.
pub fn test_listing_table_dir(data_type: &str, file_name: &str) -> Path {
    let abs_file_path = test_path(data_type, file_name);
    Path::from_absolute_path(abs_file_path).unwrap()
}

pub fn test_listing_table_url(data_type: &str) -> ListingTableUrl {
    let cwd = std::env::current_dir().unwrap().join("exon");
    let start_directory = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or(cwd);

    let abs_file_path = start_directory
        .join("test-data")
        .join("datasources")
        .join(data_type);

    let ltu = ListingTableUrl::parse(abs_file_path.to_str().unwrap()).unwrap();

    ltu
}
