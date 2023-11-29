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
use itertools::Itertools;

/// Extension trait for [`FileScanConfig`] that adds whole file repartitioning.
pub trait ExonFileScanConfig {
    /// Repartition the file groups into whole partitions.
    fn regroup_files_by_size(&self, target_partitions: usize) -> Option<Vec<Vec<PartitionedFile>>>;

    /// Get the file schema projection.
    fn file_projection(&self) -> Vec<usize>;
}

impl ExonFileScanConfig for FileScanConfig {
    fn regroup_files_by_size(&self, target_partitions: usize) -> Option<Vec<Vec<PartitionedFile>>> {
        if self.file_groups.is_empty() {
            return None;
        }

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

fn regroup_files_by_size(
    file_partitions: &[Vec<PartitionedFile>],
    target_group_size: usize,
) -> Vec<Vec<PartitionedFile>> {
    let flattened_files = file_partitions
        .iter()
        .flatten()
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .sorted_by_key(|f| f.object_meta.size)
        .collect::<Vec<_>>();

    let target_partitions = std::cmp::min(target_group_size, flattened_files.len());
    let mut new_file_groups = Vec::new();

    // Add empty file groups to the new file groups equal to the number of target partitions.
    for _ in 0..target_partitions {
        new_file_groups.push(Vec::new());
    }

    // Work through the flattened files and add them to the new file groups.
    for (i, file) in flattened_files.iter().enumerate() {
        let target_partition = i % target_partitions;
        new_file_groups[target_partition].push(file.clone());
    }

    new_file_groups
}
