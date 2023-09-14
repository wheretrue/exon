//! VCF data source.
//!
//! This module provides functionality for working with VCF files as a data source.

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

mod array_builder;
mod async_batch_reader;
mod config;
mod file_opener;
mod indexed_file_utils;
mod indexed_scanner;
mod scanner;
mod schema_builder;
mod table_provider;

mod indexed_async_record_stream;

pub use self::array_builder::VCFArrayBuilder;
pub use self::config::VCFConfig;
pub use self::file_opener::VCFOpener;
pub use self::indexed_file_utils::get_byte_range_for_file;
pub use self::indexed_scanner::IndexedVCFScanner;
pub use self::scanner::VCFScan;
pub use self::schema_builder::VCFSchemaBuilder;
pub use self::table_provider::ListingVCFTable;
pub use self::table_provider::ListingVCFTableOptions;
pub use self::table_provider::VCFListingTableConfig;
