# Download the uniprot_sprot.fasta and make files for benchmarking
download-uniprot-bench:
	mkdir -p exon/benches/data
	wget -O exon/benches/data/uniprot_sprot.fasta.gz https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.fasta.gz
	gzip -k -d exon/benches/data/uniprot_sprot.fasta.gz
	zstd -k exon/benches/data/uniprot_sprot.fasta

download-vcf-sample:
	wget -O benchmark.vcf.gz "https://ftp-trace.ncbi.nlm.nih.gov/ReferenceSamples/giab/release/AshkenazimTrio/HG002_NA24385_son/latest/GRCh38/HG002_GRCh38_1_22_v4.2.1_benchmark.vcf.gz"
	gzcat benchmark.vcf.gz | head -n 150000 | bgzip -c > exon/benches/data/benchmark.vcf.gz
	gunzip -k exon/benches/data/benchmark.vcf.gz
