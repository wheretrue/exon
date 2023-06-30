test_that("reading a FASTA works", {
    batch_reader = read_fasta_file("../../../../exon/test-data/datasources/fasta/test.fa")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("id", "description", "sequence"))

    # Check there's two rows.
    expect_equal(nrow(df), 2)
})

test_that("reading a FASTQ works", {
    batch_reader = read_fastq_file("../../../../exon/test-data/datasources/fastq/test.fq")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("name", "description", "sequence", "quality_scores"))

    # Check there's two rows.
    expect_equal(nrow(df), 2)
})

test_that("reading a GFF works", {
    batch_reader = read_gff_file("../../../../exon/test-data/datasources/gff/test.gff")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("seqid", "source", "type", "start", "end", "score", "strand", "phase", "attributes"))

    # Check there's two rows.
    expect_equal(nrow(df), 5000)
})

test_that("reading a GenBank works", {
    batch_reader = read_genbank_file("../../../../exon/test-data/datasources/genbank/test.gb")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("sequence", "accession", "comments", "contig", "date", "dblink", "definition", "division", "keywords", "molecule_type", "name", "source", "version", "topology", "features"))

    # Check there's two rows.
    expect_equal(nrow(df), 1)
})

test_that("reading a VCF works", {
    batch_reader = read_vcf_file("../../../../exon/test-data/datasources/vcf/index.vcf")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("chrom", "pos", "id", "ref", "alt", "qual", "filter", "info", "formats"))

    # Check there's two rows.
    expect_equal(nrow(df), 621)
})

test_that("reading a block gzipped VCF works", {
    batch_reader = read_vcf_file("../../../../exon/test-data/datasources/vcf/index.vcf.gz")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("chrom", "pos", "id", "ref", "alt", "qual", "filter", "info", "formats"))

    # Check there's two rows.
    expect_equal(nrow(df), 621)
})

test_that("reading a BCF works", {
    batch_reader = read_vcf_file("../../../../exon/test-data/datasources/bcf/index.bcf")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("chrom", "pos", "id", "ref", "alt", "qual", "filter", "info", "formats"))

    # Check there's two rows.
    expect_equal(nrow(df), 621)
})

test_that("reading a BED works", {
    batch_reader = read_bed_file("../../../../exon/test-data/datasources/bed/test.bed")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("reference_sequence_name", "start", "end", "name", "score", "strand", "thick_start", "thick_end", "color", "block_count", "block_sizes", "block_starts"))

    # Check there's two rows.
    expect_equal(nrow(df), 1)
})

test_that("reading a SAM works", {
    batch_reader = read_sam_file("../../../../exon/test-data/datasources/sam/test.sam")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("name", "flag", "reference", "start", "end", "mapping_quality", "cigar", "mate_reference", "sequence", "quality_score"))

    # Check there's two rows.
    expect_equal(nrow(df), 1)
})

test_that("reading a BAM works", {
    batch_reader = read_sam_file("../../../../exon/test-data/datasources/bam/test.bam")
    df = as.data.frame(batch_reader$read_table())

    # Check the column names are what's expected.
    expect_equal(colnames(df), c("name", "flag", "reference", "start", "end", "mapping_quality", "cigar", "mate_reference", "sequence", "quality_score"))

    # Check there's two rows.
    expect_equal(nrow(df), 1)
})
