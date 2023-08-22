use std::sync::Arc;

use datafusion::{
    common::tree_node::Transformed,
    datasource::listing::PartitionedFile,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{with_new_children_if_necessary, ExecutionPlan},
};

use crate::{datasources::fasta::FASTAScan, ExonSessionExt};

type FilePartitions = Vec<Vec<PartitionedFile>>;

/// Regroup the file partition into a new set of file partitions of the target size.
pub(crate) fn regroup_file_partitions(
    file_partitions: FilePartitions,
    target_group_size: usize,
) -> FilePartitions {
    let flattened_files = file_partitions
        .iter()
        .flatten()
        .cloned()
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

use datafusion::error::Result;

fn optimize_file_partitions(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let new_plan = if plan.children().is_empty() {
        Transformed::No(plan) // no children, i.e. leaf
    } else {
        Transformed::No(plan) // TODO;
    };

    let (new_plan, transformed) = new_plan.into_pair();

    if let Some(fasta_scan) = new_plan.as_any().downcast_ref::<FASTAScan>() {
        let new_scan = fasta_scan.get_repartitioned(target_partitions);

        Ok(Transformed::Yes(Arc::new(new_scan)))
    } else {
        Ok(Transformed::No(new_plan))
    }
}

#[derive(Default)]
pub struct ExonRoundRobin {}

impl PhysicalOptimizerRule for ExonRoundRobin {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        let enabled = config.optimizer.enable_round_robin_repartition;
        let repartition_file_scans = config.optimizer.repartition_file_scans;
        let target_partitions = config.execution.target_partitions;

        // let plan = if !enabled || target_partitions == 1 {
        let plan = if true {
            optimize_file_partitions(plan, target_partitions)?
        } else {
            Transformed::No(plan)
        };

        let (plan, transformed) = plan.into_pair();

        Ok(plan)
    }

    fn name(&self) -> &str {
        "exon_round_robin"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use datafusion::{
        execution::{context::SessionState, runtime_env::RuntimeEnv},
        prelude::{SessionConfig, SessionContext},
    };

    use crate::{
        datasources::{ExonFileType, ExonReadOptions},
        new_exon_config,
        tests::{test_listing_table_url, test_path},
    };

    use super::*;

    #[tokio::test]
    async fn test_regroup_file_partitions() {
        let ctx = SessionContext::new_exon();

        let file_file = ExonFileType::from_str("fasta").unwrap();

        let options = ExonReadOptions::new(file_file);

        let test_path = test_path("fasta2", "test.fasta")
            .parent()
            .unwrap()
            .to_owned();

        ctx.register_exon_table("test_fasta", test_path.to_str().unwrap(), options)
            .await
            .unwrap();

        let df = ctx.sql("SELECT * FROM test_fasta").await.unwrap();

        let plan = df.logical_plan();

        let plan = ctx.state().create_physical_plan(plan).await.unwrap();
    }
}
