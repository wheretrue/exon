//! BCF data source.
//!
//! This module provides functionality for working with BCF files as a data source.

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

mod batch_reader;
mod config;
mod file_format;
mod file_opener;
mod lazy_array_builder;
mod lazy_batch_reader;
mod scanner;

pub use self::config::BCFConfig;
pub use self::file_format::BCFFormat;
pub use self::file_opener::BCFOpener;
pub use self::scanner::BCFScan;
