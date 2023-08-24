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

use crate::optimizer::file_repartitioner::regroup_file_partitions;

/// Extension trait for [`FileScanConfig`] that adds whole file repartitioning.
pub trait ExonFileScanConfig {
    /// Repartition the file groups into whole partitions.
    fn regroup_whole_files(&self, target_partitions: usize) -> Option<Vec<Vec<PartitionedFile>>>;
}

impl ExonFileScanConfig for FileScanConfig {
    fn regroup_whole_files(&self, target_partitions: usize) -> Option<Vec<Vec<PartitionedFile>>> {
        Some(regroup_file_partitions(
            &self.file_groups,
            target_partitions,
        ))
    }
}
