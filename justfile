download-uniprot-bench:
	mkdir -p benchmarks/data
	wget -O benchmarks/data/uniprot_sprot.fasta.gz https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	gzip -k -d benchmarks/data/uniprot_sprot.fasta.gz
	zstd -k benchmarks/data/uniprot_sprot.fasta

download-bam-file:
	aws s3 cp s3://1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
	samtools index benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam

GIT_SHA := `git describe --always --abbrev=7 --dirty`

run-benchmarks:
	# Build the benchmark crate.
	cargo build --release --package benchmarks

	# Run vcf benchmarks.
	hyperfine --runs 2 --export-json benchmarks/results/vcf-query_{{GIT_SHA}}.json \
		-n bcftools \
		'bcftools view -r chr1:1-1000000 benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz chr1:1-1000000 | wc -l' \
		-n exon-vcf-query \
		'./target/release/benchmarks vcf-query -p benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz -r chr1:1-1000000'

	# Run bam benchmarks.
	hyperfine --runs 2 --export-json benchmarks/results/bam-query_{{GIT_SHA}}.json \
		-n samtools \
		'samtools view -c benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam 20:1000000-100000000' \
		-n exon-bam-query \
		'./target/release/benchmarks bam-query -p benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam -r 20:1000000-100000000'

	# Run FASTA scan benchmarks.
	hyperfine --runs 5 --export-json benchmarks/results/fasta-meth-scan_{{GIT_SHA}}.json \
		-n exon-gzip \
		'./target/release/benchmarks fasta-codon-scan -p ./benchmarks/data/uniprot_sprot.fasta.gz -c gzip' \
		-n exon-zstd \
		'./target/release/benchmarks fasta-codon-scan -p ./benchmarks/data/uniprot_sprot.fasta.zst -c zstd' \
		-n exon-no-compression \
		'./target/release/benchmarks fasta-codon-scan -p ./benchmarks/data/uniprot_sprot.fasta' \
		-n biopython-gzip \
		'python benchmarks/biopython_scan_fasta.py benchmarks/data/uniprot_sprot.fasta.gz' \
		-n biopython-no-compression \
		'python benchmarks/biopython_scan_fasta.py benchmarks/data/uniprot_sprot.fasta'

plot-benchmarks:
	python benchmarks/plot_results.py

coverage:
	cargo tarpaulin --out Html
	open tarpaulin-report.html
