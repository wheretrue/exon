# How to Release `Exon`

Exon is a packaged in multiple crates as part of a larger workspace. The dependency graph looks like:

```mermaid
flowchart TD
    exon-sam --> exon-bam
    exon-sam --> exon-core
    exon-bam --> exon-core
    exon-bed --> exon-core
    exon-common --> exon-core
    exon-fasta --> exon-core
    exon-fastq --> exon-core
    exon-gff --> exon-core
    exon-gtf --> exon-core
    exon-io --> exon-core
    exon-core --> exon-exome
    exon-test --dev-dep--> exon-fasta
    exon-test --dev-dep--> exon-fastq
    exon-test --dev-dep--> exon-gff
    exon-core --> exon-cli
    exon-core --> exon-benchmark
```

Therefore, to release the packages you must first release the undepended crates, then the depended crates.

## Release Steps

First, bump the version in `Cargo.toml`, and bump the version of and inter-crate dependencies.

Commit the changes,

```console
git commit -m 'release: bump version to v0.1.0'
```

and tag the commit with the version number.

```console
git tag -a v0.1.0 -m "Release v0.1.0"
```

Then publish the crates:

```console
# Crates that do not depend on other crates
cargo publish --manifest-path exon/exon-bam/Cargo.toml
cargo publish --manifest-path exon/exon-bed/Cargo.toml
cargo publish --manifest-path exon/exon-common/Cargo.toml
cargo publish --manifest-path exon/exon-fasta/Cargo.toml
cargo publish --manifest-path exon/exon-fastq/Cargo.toml
cargo publish --manifest-path exon/exon-gff/Cargo.toml
cargo publish --manifest-path exon/exon-gtf/Cargo.toml
cargo publish --manifest-path exon/exon-io/Cargo.toml

# Crates that depend on other crates
cargo publish --manifest-path exon/exon-core/Cargo.toml
cargo publish --manifest-path exon/exon-exome/Cargo.toml
```
