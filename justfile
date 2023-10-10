download-uniprot-bench:
	mkdir -p exon-benchmarks/data
	wget -O exon-benchmarks/data/uniprot_sprot.fasta.gz https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	gzip -k -d exon-benchmarks/data/uniprot_sprot.fasta.gz
	zstd -k exon-benchmarks/data/uniprot_sprot.fasta

download-bam-file:
	aws s3 cp s3://1000genomes/phase3/data/HG00096/alignment/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam
	samtools index exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam

download-fixtures:
	aws s3 cp --recursive s3://wtt-01-dist-prd/chr17/ exon/test-data/fixtures/

GIT_SHA := `git describe --always --abbrev=7 --dirty`

run-benchmarks:
	# Build the benchmark crate.
	cargo build --profile profiling --package exon-benchmarks \

	# Run vcf benchmarks.
	hyperfine --warmup 3 --runs 5 --export-json exon-benchmarks/results/vcf-query_{{GIT_SHA}}.json \
		-n bcftools \
		"bcftools query -r chr1:10000-10000000 -f '\n' exon-benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz | wc -l" \
		-n exon-vcf-query \
		'./target/profiling/exon-benchmarks vcf-query -p exon-benchmarks/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz -r chr1:10000-10000000'

	hyperfine --warmup 3 --runs 5 --export-json exon-benchmarks/results/vcf-chr17-query_{{GIT_SHA}}.json \
		-n bcftools \
		"bcftools query -r 17:100-10000000 -f '%CHROM\n' ./exon-benchmarks/data/chr17/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz | wc -l" \
		-n exon-vcf-query \
		'./target/profiling/exon-benchmarks vcf-query -p ./exon-benchmarks/data/chr17/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz -r 17:100-10000000' \
		-n exon-vcf-query-two-files \
		'./target/profiling/exon-benchmarks vcf-query -p ./exon-benchmarks/data/chr17/ -r 17:100-10000000'

	# Run multiple file.
	hyperfine --runs 2 --export-json exon-benchmarks/results/bam-scan-{{GIT_SHA}}.json \
		-n samtools \
		'samtools view -c exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam' \
		-n exon-bam-query \
		'./target/profiling/exon-benchmarks bam-query -p exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam -r 20:1000000-100000000'

	# Run vcf s3 benchmarks.
	hyperfine --warmup 1 --runs 1 --export-json exon-benchmarks/results/vcf-s3-query_{{GIT_SHA}}.json \
		-n bcftools \
		"bcftools query -r 17:1-1000000 -f '%CHROM\n' s3://1000genomes/phase1/analysis_results/integrated_call_sets/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz | wc -l" \
		-n exon-vcf-query \
		'./target/profiling/exon-benchmarks vcf-query -p s3://1000genomes/phase1/analysis_results/integrated_call_sets/ALL.chr17.integrated_phase1_v3.20101123.snps_indels_svs.genotypes.vcf.gz -r 17:1-1000000'

	hyperfine --warmup 1 --runs 1 --export-json exon-benchmarks/results/bam-s3-query_{{GIT_SHA}}.json \
		-n exon-bam-s3-query \
		'./target/profiling/exon-benchmarks bam-query -p s3://com.wheretrue.exome/cyt_assist_10x/CytAssist_FFPE_Human_Colon_Post_Xenium_Rep1_possorted_genome_bam.bam -r chr1:100000-1000000' \
		-n samtools \
		'samtools view -c s3://com.wheretrue.exome/cyt_assist_10x/CytAssist_FFPE_Human_Colon_Post_Xenium_Rep1_possorted_genome_bam.bam chr1:100000-1000000'

	# Run bam benchmarks.
	hyperfine --runs 2 --export-json exon-benchmarks/results/bam-query_{{GIT_SHA}}.json \
		-n samtools \
		'samtools view -c exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam' \
		-n exon-bam-query \
		'./target/profiling/exon-benchmarks bam-scan -p exon-benchmarks/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam -r 20:1000000-100000000'

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
