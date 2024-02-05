downoad-uniprot-bench: exon-benchmarks/data/uniprot_sprot.fasta
	mkdir -p exon-benchmarks/data
	wget -O exon-benchmarks/data/uniprot_sprot.fasta.gz https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	gzip -k -d exon-benchmarks/data/uniprot_sprot.fasta.gz
	zstd -k exon-benchmarks/data/uniprot_sprot.fasta

download-bam-file: exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
	mkdir -p exon-benchmarks/data
	aws s3 cp s3://1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
	samtools index exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam

download-fixtures: exon/exon-core/test-data/fixtures/
	mkdir -p exon/exon-core/test-data/fixtures
	aws s3 cp --recursive s3://wtt-01-dist-prd/chr17/ exon/exon-core/test-data/fixtures/

.PHONY: download-files
download-files: download-uniprot-bench download-bam-file download-fixtures

.PHONY: test
test:
	bash ./bin/test.sh

.PHONY: coverage
coverage:
	cargo tarpaulin --out Html

TAG ?= latest

.PHONY: benchmarks
benchmarks:
	python exon-benchmarks/run_benchmarks.py --tags $(TAG)
