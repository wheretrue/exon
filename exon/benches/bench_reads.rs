use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::{
    datasource::file_format::file_type::FileCompressionType, prelude::SessionContext,
};
use exon::context::ExonSessionExt;

const UNIPROT_SEQ_COUNT: usize = 569516;
const VCF_RECORD_COUNT: usize = 149770;

async fn count_seqs(file_path: &str, file_compression: Option<FileCompressionType>) {
    let ctx = SessionContext::new();

    let df = ctx.read_fasta(file_path, file_compression).await.unwrap();

    let cnt = df.count().await.unwrap();
    assert_eq!(cnt, UNIPROT_SEQ_COUNT);
}

async fn count_vcf_records(file_path: &str) {
    let ctx = SessionContext::new();

    let df = ctx.read_inferred_exon_table(file_path).await.unwrap();

    let cnt = df.count().await.unwrap();

    assert_eq!(cnt, VCF_RECORD_COUNT);
}

fn benchmark(c: &mut Criterion) {
    let mut vcf_benches = c.benchmark_group("vcf-benches");
    vcf_benches.sample_size(10);
    vcf_benches.throughput(criterion::Throughput::Elements(VCF_RECORD_COUNT as u64));

    vcf_benches.bench_function("count_vcf_rows_bgzip", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            let mut test_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            test_path.push_str("/benches/data/benchmark.vcf.gz");
            count_vcf_records(&test_path).await
        });
    });

    vcf_benches.bench_function("count_vcf_rows_uncompressed", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            let mut test_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            test_path.push_str("/benches/data/benchmark.vcf");
            count_vcf_records(&test_path).await
        });
    });

    vcf_benches.finish();

    let mut uniprot_benches = c.benchmark_group("uniprot-benches");
    uniprot_benches.sample_size(10);
    uniprot_benches.throughput(criterion::Throughput::Elements(UNIPROT_SEQ_COUNT as u64));

    uniprot_benches.bench_function("count_seqs_gzip", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            let mut test_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            test_path.push_str("/benches/data/uniprot_sprot.fasta.gz");
            count_seqs(&test_path, Some(FileCompressionType::GZIP)).await
        });
    });

    uniprot_benches.bench_function("count_seq_zst", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            let mut test_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            test_path.push_str("/benches/data/uniprot_sprot.fasta.zst");
            count_seqs(&test_path, Some(FileCompressionType::ZSTD)).await
        });
    });

    uniprot_benches.bench_function("count_seq_uncompressed", |b| {
        b.to_async(
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap(),
        )
        .iter(|| async {
            let mut test_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
            test_path.push_str("/benches/data/uniprot_sprot.fasta");
            count_seqs(&test_path, Some(FileCompressionType::UNCOMPRESSED)).await
        });
    });

    uniprot_benches.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
