# TCA

TCA is not ready for public use. It is still in development.

## File Formats

| Format    | Compression(s) | Inferred Extension(s) |
| --------- | -------------- | --------------------- |
| BAM       | -              | .bam                  |
| BCF       | -              | .bcf                  |
| BED       | gz, zstd       | .bed                  |
| FASTA     | gz, zstd       | .fasta, .fa, .fna     |
| FASTQ     | gz, zstd       | .fastq, .fq           |
| GENBANK   | gz, zstd       | .gbk, .genbank, .gb   |
| GFF       | gz, zstd       | .gff                  |
| HMMDOMTAB | gz, zstd       | .hmmdomtab            |
| MZML      | gz, zstd       | .mzml[^2]             |
| SAM       | -              | .sam                  |
| VCF       | gz[^1]         | .vcf                  |


[^1]: Uses bgzip not gzip.
[^2]: mzML also works.
