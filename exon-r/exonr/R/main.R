#' Defines the main functions for the exonr package.

library(arrow)
library(nanoarrow)

#' Read FASTA File
#'
#' This function reads a FASTA file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the FASTA file.
#'
#' @return A RecordBatch stream representing the contents of the FASTA table.
#'
#' @export
read_fasta_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table(file_path, pointer_addr)

    return(RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read FASTQ File
#'
#' This function reads a FASTQ file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the FASTQ file.
#'
#' @return A RecordBatch stream representing the contents of the FASTQ table.
#'
#' @export
read_fastq_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table(file_path, pointer_addr)

    return(RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read GFF File
#'
#' This function reads a GFF file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the GFF file.
#'
#' @return A RecordBatch stream representing the contents of the GFF table.
#'
#' @export
read_gff_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table(file_path, pointer_addr)

    return(RecordBatchStreamReader$import_from_c(pointer_addr))
}
