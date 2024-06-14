// Copyright 2024 WHERE TRUE Technologies.
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

//! GFF Datasource Module
//!
//! This module provides functionality for working with GFF files as a data source.

mod file_opener;
mod indexed_file_opener;
mod indexed_scanner;
mod scanner;

/// Table provider for GFF files.
pub mod table_provider;

pub use self::file_opener::GFFOpener;
pub use self::scanner::GFFScan;

mod udtf;
pub use self::udtf::GFFIndexedScanFunction;
pub use self::udtf::GFFScanFunction;
