use std::sync::Arc;

use datafusion::{
    common::tree_node::Transformed,
    datasource::listing::PartitionedFile,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{with_new_children_if_necessary, ExecutionPlan},
};

use crate::datasources::fasta::FASTAScan;

type FilePartitions = Vec<Vec<PartitionedFile>>;

/// Regroup the file partition into a new set of file partitions of the target size.
pub(crate) fn regroup_file_partitions(
    file_partitions: &FilePartitions,
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

fn optimize_file_partitions(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let new_plan = if plan.children().is_empty() {
        Transformed::No(plan) // no children, i.e. leaf
    } else {
        let children = plan
            .children()
            .iter()
            .map(|child| {
                let optimized = optimize_file_partitions(child.clone(), target_partitions)
                    .map(Transformed::into);

                optimized
            })
            .collect::<Result<_>>()?;

        with_new_children_if_necessary(plan, children)?
    };

    let (new_plan, _transformed) = new_plan.into_pair();

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
        let target_partitions = config.execution.target_partitions;

        // let plan = if !enabled || target_partitions == 1 {
        let plan = if true {
            optimize_file_partitions(plan, target_partitions)?
        } else {
            Transformed::No(plan)
        };

        let (plan, _transformed) = plan.into_pair();

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
    use std::str::FromStr;

    use datafusion::{physical_plan::joins::HashJoinExec, prelude::SessionContext};

    use crate::{
        datasources::{fasta::FASTAScan, ExonFileType, ExonReadOptions},
        tests::test_path,
        ExonSessionExt,
    };

    #[tokio::test]
    async fn test_regroup_file_partitions() {
        let ctx = SessionContext::new_exon();

        let file_file = ExonFileType::from_str("fasta").unwrap();

        let options = ExonReadOptions::new(file_file);

        let test_path = test_path("repartition-test", "test.fasta")
            .parent()
            .unwrap()
            .to_owned();

        ctx.register_exon_table("test_fasta", test_path.to_str().unwrap(), options)
            .await
            .unwrap();

        let df = ctx.sql("SELECT * FROM test_fasta").await.unwrap();

        let plan = df.logical_plan();

        let plan = ctx.state().create_physical_plan(plan).await.unwrap();

        let scan = plan.as_any().downcast_ref::<FASTAScan>().unwrap();

        // Assert we have two file groups vs the default one
        assert_eq!(scan.base_config.file_groups.len(), 2);

        // Test a subquery
        let df = ctx
            .sql("SELECT * FROM test_fasta JOIN (SELECT * FROM test_fasta) AS t2 ON t2.id = test_fasta.id")
            .await
            .unwrap();

        let plan = df.logical_plan();
        let plan = ctx.state().create_physical_plan(plan).await.unwrap();

        let hash_join_exec = plan.as_any().downcast_ref::<HashJoinExec>().unwrap();

        let left_fasta = hash_join_exec
            .left()
            .as_any()
            .downcast_ref::<FASTAScan>()
            .unwrap();
        assert_eq!(left_fasta.base_config.file_groups.len(), 2);

        let right_fasta = hash_join_exec
            .right()
            .as_any()
            .downcast_ref::<FASTAScan>()
            .unwrap();

        // Assert we have two file groups vs the default one
        assert_eq!(right_fasta.base_config.file_groups.len(), 2);
    }
}
