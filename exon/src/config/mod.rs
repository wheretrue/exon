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

use datafusion::{config::ConfigOptions, prelude::SessionConfig};

pub const BATCH_SIZE: usize = 8 * 1024;

/// Create a new [`SessionConfig`] for the exon.
pub fn new_exon_config() -> SessionConfig {
    let mut options = ConfigOptions::new();
    options.execution.parquet.pushdown_filters = true;
    options.execution.parquet.reorder_filters = true;
    options.optimizer.repartition_sorts = true;

    SessionConfig::from(options)
        .with_batch_size(BATCH_SIZE)
        .with_create_default_catalog_and_schema(true)
        .with_default_catalog_and_schema("public", "exon")
        .with_information_schema(true)
        .with_repartition_aggregations(true)
        .with_repartition_joins(true)
        .with_repartition_windows(true)
        .with_target_partitions(num_cpus::get())
}
