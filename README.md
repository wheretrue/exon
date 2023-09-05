<h1 align="center">
    <img src="https://raw.githubusercontent.com/wheretrue/exon/main/.github/images/logo.svg" width="220px" alt="Exon" />
</h1>

Exon is an analysis toolkit for life-science applications. It features:

* Support for many file formats from bioinformatics, proteomics, and others
* Local filesystem and object storage support
* Arrow FFI primitives for multi-language support
* SQL based access to bioinformatics data -- general DML and some DDL support

Please note Exon was recently excised from a larger library, so please be patient as we work to clean up after that. If you have a comment or question in the meantime, please file an issue.

* [Installation](#installation)
* [Usage](#usage)
* [File Formats](#file-formats)
* [Settings](#settings)
* [Benchmarks](#benchmarks)

## Installation

Exon is available via crates.io. To install, run:

```bash
cargo add exon
```

## Usage

Exon is designed to be used as a library. For example, to read a FASTA file:

```rust
use exon::context::ExonSessionExt;

use datafusion::prelude::*;
use datafusion::error::Result;

let ctx = SessionContext::new_exon();

let df = ctx.read_fasta("test-data/datasources/fasta/test.fasta", None).await?;
```

Please see the [rust docs](https://docs.rs/exon) for more information.

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
| GTF       | gz, zstd       | .gtf                  |
| HMMDOMTAB | gz, zstd       | .hmmdomtab            |
| MZML      | gz, zstd       | .mzml[^2]             |
| SAM       | -              | .sam                  |
| VCF       | gz[^1]         | .vcf                  |

[^1]: Uses bgzip not gzip.
[^2]: mzML also works.

## Settings

Exon using the following settings:

| Setting | Default | Description |
| ------- | ------- | ----------- |
| `exon.parse_vcf_info` | `true` | Parse VCF INFO fields. If False, INFO fields will be returned as a single string. |
| `exon.parse_vcf_format` | `true` | Parse VCF FORMAT fields. If False, FORMAT fields will be returned as a single string. |

You can update the settings by running:

```sql
SET <setting> = <value>;
```

For example, to disable parsing of VCF INFO fields:

```sql
SET exon.parse_vcf_info = false;
```

## Benchmarks

Please see the [benchmarks](exon-benchmarks) [README](exon-benchmarks/README.md) for more information.
