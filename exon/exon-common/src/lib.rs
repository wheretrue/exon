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
//! Common utilities for Exon.

mod object_store_files_from_table_path;

mod array_builder;
mod table_schema;

pub use array_builder::ExonArrayBuilder;
pub use object_store_files_from_table_path::object_store_files_from_table_path;
pub use table_schema::TableSchema;
pub use table_schema::TableSchemaBuilder;

/// Default batch size for reading and writing.
pub const DEFAULT_BATCH_SIZE: usize = 8 * 1024;
