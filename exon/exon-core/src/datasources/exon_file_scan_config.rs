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

use datafusion::datasource::{listing::PartitionedFile, physical_plan::FileScanConfig};

use crate::physical_optimizer::file_repartitioner::regroup_files_by_size;

/// Extension trait for [`FileScanConfig`] that adds whole file repartitioning.
pub trait ExonFileScanConfig {
    /// Repartition the file groups into whole partitions.
    fn regroup_files_by_size(&self, target_partitions: usize) -> Option<Vec<Vec<PartitionedFile>>>;

    /// Get the file schema projection.
    fn file_projection(&self) -> Vec<usize>;
}

impl ExonFileScanConfig for FileScanConfig {
    fn regroup_files_by_size(&self, target_partitions: usize) -> Option<Vec<Vec<PartitionedFile>>> {
        Some(regroup_files_by_size(&self.file_groups, target_partitions))
    }

    fn file_projection(&self) -> Vec<usize> {
        let n_file_schema_fields = self.file_schema.fields().len();

        self.projection
            .as_ref()
            .map(|p| {
                p.iter()
                    .filter(|f| **f < n_file_schema_fields)
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .unwrap_or_else(|| (0..n_file_schema_fields).collect())
    }
}
