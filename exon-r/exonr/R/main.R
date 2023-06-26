# Import the wrappers

require(arrow)

read_fasta_table <- function() {
    stream_ptr <- arrow$allocate_arrow_array_stream()

    # stream_ptr <- arrow::RecordBatch$import_from_c()

    hello_world()
}
