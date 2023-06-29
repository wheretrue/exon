# Import the wrappers

library(arrow)
library(nanoarrow)

read_fasta_table <- function(file_path) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()

    read_fasta_file_extendr(
        file_path,
        nanoarrow::nanoarrow_pointer_addr_chr(stream)
    )

    record_batch_reader <- RecordBatchStreamReader$import_from_c(
        nanoarrow::nanoarrow_pointer_addr_chr(stream)
    )

    tab <- record_batch_reader$read_table()
    df <- as.data.frame(tab)

    print(df)
}
