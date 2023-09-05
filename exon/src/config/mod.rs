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

use std::sync::Arc;

use datafusion::{
    config::{ConfigExtension, ConfigOptions, ExtensionOptions},
    prelude::SessionConfig,
};

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
        .with_extension(Arc::new(ExonConfigExtension::default()))
}

#[derive(Debug, Clone)]
pub struct ExonConfigExtension {
    /// If true, the VCF parser will parse the INFO field into a struct.
    pub parse_vcf_info: bool,

    /// If true, the VCF parser will parse the FORMAT field into a list of structs.
    pub parse_vcf_format: bool,
}

impl Default for ExonConfigExtension {
    fn default() -> Self {
        Self {
            parse_vcf_info: true,
            parse_vcf_format: true,
        }
    }
}

impl ConfigExtension for ExonConfigExtension {
    const PREFIX: &'static str = "exon";
}

impl ExtensionOptions for ExonConfigExtension {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(Self {
            parse_vcf_info: self.parse_vcf_info,
            parse_vcf_format: self.parse_vcf_format,
        })
    }

    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        match key {
            "parse_vcf_info" => {
                self.parse_vcf_info = value.parse().map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Could not parse value for {}: {}",
                        key, e
                    ))
                })?;
                Ok(())
            }
            "parse_vcf_format" => {
                self.parse_vcf_format = value.parse().map_err(|e| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "Could not parse value for {}: {}",
                        key, e
                    ))
                })?;
                Ok(())
            }
            _ => Err(datafusion::error::DataFusionError::NotImplemented(format!(
                "Unknown option: {}",
                key
            ))),
        }
    }

    fn entries(&self) -> Vec<datafusion::config::ConfigEntry> {
        vec![
            datafusion::config::ConfigEntry {
                key: "parse_vcf_info".to_string(),
                value: Some("false".to_string()),
                description: "If true, the VCF parser will parse the INFO field into a struct.",
            },
            datafusion::config::ConfigEntry {
                key: "parse_vcf_format".to_string(),
                value: Some("false".to_string()),
                description:
                    "If true, the VCF parser will parse the FORMAT field into a list of structs.",
            },
        ]
    }
}
