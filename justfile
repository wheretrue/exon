download-uniprot-bench:
	mkdir -p exon-benchmarks/data
	wget -O exon-benchmarks/data/uniprot_sprot.fasta.gz https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	gzip -k -d exon-benchmarks/data/uniprot_sprot.fasta.gz
	zstd -k exon-benchmarks/data/uniprot_sprot.fasta

download-bam-file:
	aws s3 cp s3://1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
	samtools index exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam

GIT_SHA := `git describe --always --abbrev=7 --dirty`

run-benchmarks:
	# Build the benchmark crate.
	cargo build --profile profiling --package exon-benchmarks \

	# Run vcf benchmarks.
	hyperfine --warmup 5 --runs 5 --export-json exon-benchmarks/results/vcf-query_{{GIT_SHA}}.json \
		-n bcftools \
		'bcftools view -r chr1:1-1000000 exon-benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz chr1:1-1000000 | wc -l' \
		-n exon-vcf-query \
		'./target/profiling/exon-benchmarks vcf-query -p exon-benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz -r chr1:1-1000000'

	# Run bam benchmarks.
	hyperfine --runs 2 --export-json exon-benchmarks/results/bam-query_{{GIT_SHA}}.json \
		-n samtools \
		'samtools view -c exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam 20:1000000-100000000' \
		-n exon-bam-query \
		'./target/profiling/exon-benchmarks bam-query -p exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam -r 20:1000000-100000000'

	# Run FASTA scan benchmarks.
	hyperfine --runs 5 --export-json exon-benchmarks/results/fasta-meth-scan_{{GIT_SHA}}.json \
		-n exon-gzip \
		'./target/profiling/exon-benchmarks fasta-codon-scan -p ./exon-benchmarks/data/uniprot_sprot.fasta.gz -c gzip' \
		-n exon-zstd \
		'./target/profiling/exon-benchmarks fasta-codon-scan -p ./exon-benchmarks/data/uniprot_sprot.fasta.zst -c zstd' \
		-n exon-no-compression \
		'./target/profiling/exon-benchmarks fasta-codon-scan -p ./exon-benchmarks/data/uniprot_sprot.fasta' \
		-n biopython-gzip \
		'python exon-benchmarks/biopython_scan_fasta.py exon-benchmarks/data/uniprot_sprot.fasta.gz' \
		-n biopython-no-compression \
		'python exon-benchmarks/biopython_scan_fasta.py exon-benchmarks/data/uniprot_sprot.fasta'

	# Run FASTA parallel scan benchmarks.
	hyperfine --runs 5 --export-json exon-benchmarks/results/fasta-parallel-scan_{{GIT_SHA}}.json \
		-n workers-1 \
		'./target/profiling/exon-benchmarks fasta-scan-parallel -p ./exon-benchmarks/data/fasta-files -w 1' \
		-n workers-2 \
		'./target/profiling/exon-benchmarks fasta-scan-parallel -p ./exon-benchmarks/data/fasta-files -w 2' \
		-n workers-4 \
		'./target/profiling/exon-benchmarks fasta-scan-parallel -p ./exon-benchmarks/data/fasta-files -w 4' \
		-n workers-6 \
		'./target/profiling/exon-benchmarks fasta-scan-parallel -p ./exon-benchmarks/data/fasta-files -w 6' \
		-n workers-8 \
		'./target/profiling/exon-benchmarks fasta-scan-parallel -p ./exon-benchmarks/data/fasta-files -w 8'

run-mzml-benchmarks:
	# Run mzml scan benchmarks.
	hyperfine --runs 5 --export-json exon-benchmarks/results/mzml-scan_{{GIT_SHA}}.json \
		-n exon-gzip \
		'./target/profiling/exon-benchmarks mz-ml-scan -p ./exon-benchmarks/data/SALJA0984.mzML.gz -c gzip' \
		-n exon-no-compression \
		'./target/profiling/exon-benchmarks mz-ml-scan -p ./exon-benchmarks/data/SALJA0984.mzML' \
		-n pymzml-gzip \
		'python exon-benchmarks/pymzml_scan.py exon-benchmarks/data/SALJA0984.mzML.gz' \
		-n pymzml-no-compression \
		'python exon-benchmarks/pymzml_scan.py exon-benchmarks/data/SALJA0984.mzML' \
		-n pyteomics-no-compression \
		'python exon-benchmarks/pyteomics_scan.py exon-benchmarks/data/SALJA0984.mzML'

plot-benchmarks:
	python exon-benchmarks/plot_results.py

coverage:
	cargo tarpaulin --out Html
	open tarpaulin-report.html
