#' @name exonr
#' @title ExonR

library(arrow)
library(nanoarrow)
library(R6)

#' Copy the inferred exon table from the given path into the given stream.
#'
#' @param file_path The path to the inferred exon table.
#' @param stream_ptr The pointer to the stream to copy the inferred exon table
read_inferred_exon_table_r <- function(file_path, pointer_addr) {
    result <- read_inferred_exon_table(file_path, pointer_addr)

    # if result$err is not null, then we have an error so throw it
    if (!is.null(result$err)) {
        stop(result$err)
    }
}

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

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
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

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
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

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read GenBank File
#'
#' This function reads a GenBank file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the GenBank file.
#'
#' @return A RecordBatch stream representing the contents of the GenBank table.
#'
#' @export
read_genbank_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read VCF/BCF File
#'
#' This function reads a VCF/BCF file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the VCF/BCF file.
#'
#' @return A RecordBatch stream representing the contents of the VCF/BCF file.
#'
#' @export
read_vcf_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read BED File
#'
#' This function reads a BED file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the BED file.
#'
#' @return A RecordBatch stream representing the contents of the BED table.
#'
#' @export
read_bed_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read SAM/BAM File
#'
#' This function reads a SAM file from a file and returns a RecordBatch
#' stream. It will also read BAM files.
#'
#' @param file_path The path to the SAM/BAM file.
#'
#' @return A RecordBatch stream representing the contents of the SAM/BAM file.
#'
#' @export
read_sam_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
}

#' Read MzML File
#'
#' This function reads a MzML file from a file and returns a RecordBatch
#' stream.
#'
#' @param file_path The path to the MzML file.
#'
#' @return A RecordBatch stream representing the contents of the MzML file.
#'
#' @export
read_mzml_file <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

    read_inferred_exon_table_r(file_path, pointer_addr)

    return(arrow::RecordBatchStreamReader$import_from_c(pointer_addr))
}


ExonDataFrame <- R6Class("ExonDataFrame",
    public = list(
        initialize = function(result) {
            private$data_frame <- result
        },
        to_arrow = function() {
            stream <- nanoarrow::nanoarrow_allocate_array_stream()
            pointer_addr <- nanoarrow::nanoarrow_pointer_addr_chr(stream)

            private$data_frame$to_arrow(pointer_addr)

            arrow::RecordBatchStreamReader$import_from_c(pointer_addr)
        }
    ),
    private = list(
        data_frame = NULL
    )
)

ExonRSessionContext <- R6Class("ExonRSessionContext",
    public = list(
        initialize = function() {
            private$exon_session_context <- ExonSessionContext$new()
        },
        sql = function(query) {
            df <- private$exon_session_context$sql(query)

            return(ExonDataFrame$new(df))
        }
    ),
    private = list(
        exon_session_context = NULL
    )
)
