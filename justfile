# Download the uniprot_sprot.fasta and make files for benchmarking
download-uniprot-bench:
	mkdir -p benchmarks/data
	wget -O benchmarks/data/uniprot_sprot.fasta.gz https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	gzip -k -d benchmarks/data/uniprot_sprot.fasta.gz
	zstd -k benchmarks/data/uniprot_sprot.fasta

run-benchmarks:
	cargo build --release --package benchmarks
	hyperfine --runs 2 \
		'bcftools view -r chr1:1-1000000 benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz chr1:1-1000000 | wc -l' \
		'./target/release/benchmarks vcf-query -p benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz -r chr1:1-1000000'

coverage:
	cargo tarpaulin --out Html
	open tarpaulin-report.html
