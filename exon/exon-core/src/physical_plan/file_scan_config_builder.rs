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

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::{
    datasource::{listing::PartitionedFile, physical_plan::FileScanConfig},
    execution::object_store::ObjectStoreUrl,
    physical_expr::PhysicalSortExpr,
    physical_plan::Statistics,
};

/// A builder for `FileScanConfig`.
pub struct FileScanConfigBuilder {
    object_store_url: ObjectStoreUrl,
    file_schema: SchemaRef,
    file_groups: Vec<Vec<PartitionedFile>>,
    statistics: Statistics,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    output_ordering: Vec<Vec<PhysicalSortExpr>>,
    table_partition_cols: Vec<(String, DataType)>,
    infinite_source: bool,
}

impl FileScanConfigBuilder {
    /// Create a new builder.
    pub fn new(
        object_store_url: ObjectStoreUrl,
        file_schema: SchemaRef,
        file_groups: Vec<Vec<PartitionedFile>>,
    ) -> Self {
        Self {
            object_store_url,
            file_schema,
            file_groups,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            output_ordering: Vec::new(),
            table_partition_cols: Vec::new(),
            infinite_source: false,
        }
    }

    /// Set the projection from an Option.
    pub fn projection_option(mut self, projection: Option<Vec<usize>>) -> Self {
        self.projection = projection;
        self
    }

    /// Set from the limit from an Option.
    pub fn limit_option(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    /// Build a `FileScanConfig` from the current state of the builder.
    pub fn build(self) -> FileScanConfig {
        FileScanConfig {
            object_store_url: self.object_store_url,
            file_schema: self.file_schema,
            file_groups: self.file_groups,
            statistics: self.statistics,
            projection: self.projection,
            limit: self.limit,
            output_ordering: self.output_ordering,
            table_partition_cols: self.table_partition_cols,
            infinite_source: self.infinite_source,
        }
    }
}
