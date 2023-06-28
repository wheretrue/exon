# Import the wrappers

require(arrow)

read_fasta_table <- function(file_path) {
    stream_ptr <- arrow$allocate_arrow_array_stream()

    batch_reader_ptr <- read_fasta_file_extendr(file_path, stream_ptr)

    record_batch_reader <- RecordBatchStreamReader$import_from_c(batch_reader_ptr)

    print(record_batch_reader)
}
