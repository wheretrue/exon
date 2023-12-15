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

//! Data source for BAM files.
//!
//! Reads BAM files. It is the binary version of SAM files.

mod file_opener;
mod indexed_file_opener;
mod indexed_scanner;
mod scanner;

/// Table provider for BAM files.
pub mod table_provider;

pub use file_opener::BAMOpener;
pub use indexed_scanner::IndexedBAMScan;
pub use scanner::BAMScan;

mod udtf;
pub use udtf::BAMIndexedScanFunction;
pub use udtf::BAMScanFunction;
