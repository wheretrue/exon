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

mod file_opener;
mod scanner;
mod udtfs;

/// Table provider for FASTA files.
pub mod table_provider;

mod indexed_file_opener;
pub(crate) mod indexed_scanner;

pub use self::file_opener::FASTAOpener;
pub use self::scanner::FASTAScan;
pub use self::udtfs::fasta_indexed_scan::FastaIndexedScanFunction;
pub use self::udtfs::fasta_scan::FastaScanFunction;
