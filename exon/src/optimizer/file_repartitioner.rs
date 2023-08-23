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
    common::tree_node::Transformed,
    datasource::listing::PartitionedFile,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{with_new_children_if_necessary, ExecutionPlan},
};

use crate::datasources::{
    bed::BEDScan, fasta::FASTAScan, fastq::FASTQScan, gff::GFFScan, gtf::GTFScan,
    hmmdomtab::HMMDomTabScan,
};

#[cfg(feature = "genbank")]
use crate::datasources::genbank::GenbankScan;

#[cfg(feature = "mzml")]
use crate::datasources::mzml::MzMLScan;

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

    if let Some(bed_scan) = new_plan.as_any().downcast_ref::<BEDScan>() {
        let new_scan = bed_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    if let Some(fasta_scan) = new_plan.as_any().downcast_ref::<FASTAScan>() {
        let new_scan = fasta_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    if let Some(fastq_scan) = new_plan.as_any().downcast_ref::<FASTQScan>() {
        let new_scan = fastq_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    // TODO: Add FCS support
    // #[cfg(feature = "fcs")]
    // if let Some(fcs_scan) = new_plan.as_any().downcast_ref::<FCSScan>() {
    //     let new_scan = fcs_scan.get_repartitioned(target_partitions);

    //     return Ok(Transformed::Yes(Arc::new(new_scan)));
    // }

    #[cfg(feature = "genbank")]
    if let Some(genbank_scan) = new_plan.as_any().downcast_ref::<GenbankScan>() {
        let new_scan = genbank_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    if let Some(gff_scan) = new_plan.as_any().downcast_ref::<GFFScan>() {
        let new_scan = gff_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    if let Some(gtf_scan) = new_plan.as_any().downcast_ref::<GTFScan>() {
        let new_scan = gtf_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    if let Some(hmm_scan) = new_plan.as_any().downcast_ref::<HMMDomTabScan>() {
        let new_scan = hmm_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    #[cfg(feature = "mzml")]
    if let Some(mzml_scan) = new_plan
        .as_any()
        .downcast_ref::<crate::datasources::mzml::MzMLScan>()
    {
        let new_scan = mzml_scan.get_repartitioned(target_partitions);

        return Ok(Transformed::Yes(Arc::new(new_scan)));
    }

    Ok(Transformed::No(new_plan))
}

/// Optimizer rule that repartitions the file partitions of a plan into a new set of file partitions.
///
/// This is useful for when you have a large number of files and want to reduce the number of partitions
/// so they can be processed in parallel.
#[derive(Default)]
pub struct ExonRoundRobin {}

impl PhysicalOptimizerRule for ExonRoundRobin {
    fn optimize(
        &self,
        plan: std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        config: &datafusion::config::ConfigOptions,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        let repartition_file_scans = config.optimizer.repartition_file_scans;
        let target_partitions = config.execution.target_partitions;

        let plan = if !repartition_file_scans || target_partitions == 1 {
            Transformed::No(plan)
        } else {
            optimize_file_partitions(plan, target_partitions)?
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
