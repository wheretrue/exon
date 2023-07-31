use datafusion::datasource::listing::PartitionedFile;

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
