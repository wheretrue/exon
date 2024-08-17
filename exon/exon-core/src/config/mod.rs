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

use datafusion::{
    catalog::Session,
    common::extensions_options,
    config::{ConfigExtension, ConfigOptions},
    prelude::SessionConfig,
};

use crate::error::{ExonError, Result};

pub const BATCH_SIZE: usize = 8 * 1024;

/// Create a new [`SessionConfig`] for the exon.
pub fn new_exon_config() -> SessionConfig {
    let mut options = ConfigOptions::new();
    options.execution.parquet.pushdown_filters = true;
    options.execution.parquet.reorder_filters = true;
    options.optimizer.repartition_sorts = true;

    options.extensions.insert(ExonConfigExtension::default());

    SessionConfig::from(options)
        .with_batch_size(BATCH_SIZE)
        .with_create_default_catalog_and_schema(true)
        .with_default_catalog_and_schema("public", "exon")
        .with_information_schema(true)
        .with_repartition_aggregations(true)
        .with_repartition_joins(true)
        .with_repartition_windows(true)
        .with_repartition_file_scans(true)
        .with_target_partitions(num_cpus::get())
}

pub fn extract_config_from_state(session_state: &dyn Session) -> Result<&ExonConfigExtension> {
    let config = session_state.config();

    extract_exon_config(config)
}

pub fn extract_exon_config(session_config: &SessionConfig) -> Result<&ExonConfigExtension> {
    let config = session_config
        .options()
        .extensions
        .get::<ExonConfigExtension>()
        .ok_or(ExonError::Configuration(
            "ExonConfigExtension not found in config options".to_string(),
        ))?;

    Ok(config)
}

extensions_options! {
    /// Exon config options.
    pub struct ExonConfigExtension {
        pub vcf_parse_info: bool, default = false
        pub vcf_parse_formats: bool, default = false
        pub sam_parse_tags: bool, default = false
        pub bam_parse_tags: bool, default = false
        pub cram_parse_tags: bool, default = false
    }
}

impl ConfigExtension for ExonConfigExtension {
    const PREFIX: &'static str = "exon";
}

#[cfg(test)]
mod tests {
    use crate::{config::ExonConfigExtension, new_exon_config, ExonSession};

    #[tokio::test]
    async fn test_config_set_with_defaults() -> Result<(), Box<dyn std::error::Error>> {
        let config = new_exon_config();

        let exon_config = config
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or("ExonConfigExtension not found in config options".to_string())?;

        assert!(!exon_config.vcf_parse_info);
        assert!(!exon_config.vcf_parse_formats);
        assert!(!exon_config.sam_parse_tags);
        assert!(!exon_config.bam_parse_tags);
        assert!(!exon_config.cram_parse_tags);

        Ok(())
    }

    #[tokio::test]
    async fn test_config_after_updates() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = new_exon_config();

        let options = config.options_mut();
        options.set("exon.vcf_parse_info", "false")?;
        options.set("exon.vcf_parse_formats", "false")?;
        options.set("exon.sam_parse_tags", "true")?;
        options.set("exon.bam_parse_tags", "true")?;
        options.set("exon.cram_parse_tags", "true")?;

        let exon_config = config
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or("ExonConfigExtension not found in config options".to_string())?;

        assert!(!exon_config.vcf_parse_info);
        assert!(!exon_config.vcf_parse_formats);
        assert!(exon_config.sam_parse_tags);
        assert!(exon_config.bam_parse_tags);
        assert!(exon_config.cram_parse_tags);

        Ok(())
    }

    #[tokio::test]
    async fn test_setting_config_through_sql() -> Result<(), Box<dyn std::error::Error>> {
        let ctx = ExonSession::new_exon()?;

        ctx.session.sql("SET exon.vcf_parse_info = true").await?;
        ctx.session.sql("SET exon.vcf_parse_formats = true").await?;
        ctx.session.sql("SET exon.sam_parse_tags = true").await?;
        ctx.session.sql("SET exon.bam_parse_tags = true").await?;
        ctx.session.sql("SET exon.cram_parse_tags = true").await?;

        let state = ctx.session.state();
        let exon_config = state
            .config()
            .options()
            .extensions
            .get::<ExonConfigExtension>()
            .ok_or("ExonConfigExtension not found in config options".to_string())?;

        assert!(exon_config.vcf_parse_info);
        assert!(exon_config.vcf_parse_formats);
        assert!(exon_config.sam_parse_tags);
        assert!(exon_config.bam_parse_tags);
        assert!(exon_config.cram_parse_tags);

        Ok(())
    }
}
