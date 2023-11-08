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
    physical_plan::{
        coalesce_partitions::CoalescePartitionsExec, repartition::RepartitionExec,
        with_new_children_if_necessary, ExecutionPlan, Partitioning,
    },
};

use itertools::Itertools;

use crate::{
    datasources::{
        bam::{BAMScan, IndexedBAMScan},
        bed::BEDScan,
        fasta::FASTAScan,
        fastq::FASTQScan,
        gff::GFFScan,
        gtf::GTFScan,
        hmmdomtab::HMMDomTabScan,
        sam::SAMScan,
        vcf::{IndexedVCFScanner, VCFScan},
    },
    repartitionable::Repartitionable,
};

#[cfg(feature = "fcs")]
use crate::datasources::fcs::FCSScan;

#[cfg(feature = "genbank")]
use crate::datasources::genbank::GenbankScan;

#[cfg(feature = "mzml")]
use crate::datasources::mzml::MzMLScan;

type FilePartitions = Vec<Vec<PartitionedFile>>;

/// Regroup the file partition into a new set of file partitions of the target size.
pub(crate) fn regroup_files_by_size(
    file_partitions: &FilePartitions,
    target_group_size: usize,
) -> FilePartitions {
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

macro_rules! transform_plan {
    ($new_scan:expr, $target_partitions:expr, $config:expr) => {{
        let partitioning = Partitioning::RoundRobinBatch($target_partitions);

        let repartition = RepartitionExec::try_new($new_scan.clone(), partitioning)?;
        let coalesce_partitions = CoalescePartitionsExec::new(Arc::new(repartition));

        Ok(Transformed::Yes(Arc::new(coalesce_partitions)))
    }};
}

fn optimize_file_partitions(
    plan: Arc<dyn ExecutionPlan>,
    target_partitions: usize,
    config: &datafusion::config::ConfigOptions,
) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if target_partitions == 1 {
        return Ok(Transformed::No(plan));
    }

    let new_plan = if plan.children().is_empty() {
        Transformed::No(plan) // no children, i.e. leaf
    } else {
        let children = plan
            .children()
            .iter()
            .map(|child| {
                optimize_file_partitions(child.clone(), target_partitions, config)
                    .map(Transformed::into)
            })
            .collect::<Result<_>>()?;

        with_new_children_if_necessary(plan, children)?
    };

    let (new_plan, _transformed) = new_plan.into_pair();

    if let Some(bed_scan) = new_plan.as_any().downcast_ref::<BEDScan>() {
        let new_scan = bed_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(scan) = new_plan.as_any().downcast_ref::<FASTAScan>() {
        match scan.repartitioned(target_partitions, config)? {
            Some(new_scan) => {
                return transform_plan!(new_scan, target_partitions, config);
            }
            None => {
                return Ok(Transformed::No(new_plan));
            }
        }
    }

    if let Some(indexed_vcf_scan) = new_plan.as_any().downcast_ref::<IndexedVCFScanner>() {
        match indexed_vcf_scan.repartitioned(target_partitions, config)? {
            Some(new_scan) => {
                return transform_plan!(new_scan, target_partitions, config);
            }
            None => {
                return Ok(Transformed::No(new_plan));
            }
        }
    }

    if let Some(fastq_scan) = new_plan.as_any().downcast_ref::<FASTQScan>() {
        let new_scan = fastq_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(vcf_scan) = new_plan.as_any().downcast_ref::<VCFScan>() {
        let new_scan = vcf_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(bam_scan) = new_plan.as_any().downcast_ref::<BAMScan>() {
        let new_scan = bam_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(indexed_bam_scan) = new_plan.as_any().downcast_ref::<IndexedBAMScan>() {
        let new_scan = indexed_bam_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(sam_scan) = new_plan.as_any().downcast_ref::<SAMScan>() {
        let new_scan = sam_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    #[cfg(feature = "fcs")]
    if let Some(fcs_scan) = new_plan.as_any().downcast_ref::<FCSScan>() {
        let new_scan = fcs_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    #[cfg(feature = "genbank")]
    if let Some(genbank_scan) = new_plan.as_any().downcast_ref::<GenbankScan>() {
        let new_scan = genbank_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(gff_scan) = new_plan.as_any().downcast_ref::<GFFScan>() {
        let new_scan = gff_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(gtf_scan) = new_plan.as_any().downcast_ref::<GTFScan>() {
        let new_scan = gtf_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    if let Some(hmm_scan) = new_plan.as_any().downcast_ref::<HMMDomTabScan>() {
        let new_scan = hmm_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
    }

    #[cfg(feature = "mzml")]
    if let Some(mzml_scan) = new_plan.as_any().downcast_ref::<MzMLScan>() {
        let new_scan = mzml_scan.get_repartitioned(target_partitions);
        let coalesce_partition_exec = CoalescePartitionsExec::new(Arc::new(new_scan));

        return Ok(Transformed::Yes(Arc::new(coalesce_partition_exec)));
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
            optimize_file_partitions(plan, target_partitions, config)?
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
