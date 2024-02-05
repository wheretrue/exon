# Copyright 2024 WHERE TRUE Technologies.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""CLI script to support running exon benchmarks."""

import argparse
from pathlib import Path
from string import Template
from contextlib import contextmanager
from dataclasses import dataclass, field
import subprocess

PARENT = Path(__file__).parent

from git import Repo

BENCHMARK_CMD = [
    "hyperfine",
    "--show-output",
    "--warmup",
    "2",
    "--runs",
    "4",
    "--export-json",
    "$EXON_BENCHMARK_DIR/results/benchmarks_${tag}.json",
    # VCF query benchmarks
    "-n",
    "bcftools-query-chr1",
    "bcftools query -r chr1:10000-10000000 -f '\n' ${EXON_BENCHMARK_DIR}/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz | wc -l",
    "-n",
    "exon-vcf-query-chr1",
    "./target/profiling/exon-benchmarks vcf-query -p ${EXON_BENCHMARK_DIR}/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz -r chr1:10000-10000000",
    "-n",
    "bcftools-query-chr17",
    "bcftools query -r chr17:10000-10000000 -f '\n' ${EXON_BENCHMARK_DIR}/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz | wc -l",
    "-n",
    "exon-vcf-query-chr17",
    "./target/profiling/exon-benchmarks vcf-query -p ${EXON_BENCHMARK_DIR}/data/CCDG_14151_B01_GRM_WGS_2020-08-05_chr1.filtered.shapeit2-duohmm-phased.vcf.gz -r chr17:10000-10000000",
    "-n",
    "exon-vcf-query-two-files-chr17",
    "./target/profiling/exon-benchmarks vcf-query -p ${EXON_BENCHMARK_DIR}/data/ -r chr17:10000-10000000",
    # BAM query benchmarks
    "-n",
    "samtools-query-chr20",
    "samtools view -c ${EXON_BENCHMARK_DIR}/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam 20:1000000-100000000",
    "-n",
    "exon-bam-query-chr20",
    "./target/profiling/exon-benchmarks bam-query -p ${EXON_BENCHMARK_DIR}/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam -r 20:1000000-100000000",
    "-n",
    "samtools-query-s3",
    "samtools view -c s3://com.wheretrue.exome/cyt_assist_10x/CytAssist_FFPE_Human_Colon_Post_Xenium_Rep1_possorted_genome_bam.bam chr1:100000-1000000",
    "-n",
    "exon-bam-s3-query",
    "./target/profiling/exon-benchmarks bam-query -p s3://com.wheretrue.exome/cyt_assist_10x/CytAssist_FFPE_Human_Colon_Post_Xenium_Rep1_possorted_genome_bam.bam -r chr1:100000-1000000",
    "-n",
    "samtools",
    "samtools view -c ${EXON_BENCHMARK_DIR}/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
    "-n",
    "exon-bam-query",
    "./target/profiling/exon-benchmarks bam-scan -p ${EXON_BENCHMARK_DIR}/data/HG00096.chrom20.ILLUMINA.bwa.GBR.low_coverage.20120522.bam",
    # FASTA scan benchmarks
    "-n",
    "exon-gzip",
    "./target/profiling/exon-benchmarks fasta-codon-scan -p ${EXON_BENCHMARK_DIR}/data/uniprot_sprot.fasta.gz -c gzip",
    "-n",
    "exon-zstd",
    "./target/profiling/exon-benchmarks fasta-codon-scan -p ${EXON_BENCHMARK_DIR}/data/uniprot_sprot.fasta.zst -c zstd",
    "-n",
    "exon-no-compression",
    "./target/profiling/exon-benchmarks fasta-codon-scan -p ${EXON_BENCHMARK_DIR}/data/uniprot_sprot.fasta",
    "-n",
    "biopython-gzip",
    "python exon-benchmarks/biopython_scan_fasta.py ${EXON_BENCHMARK_DIR}/data/uniprot_sprot.fasta.gz",
    "-n",
    "biopython-no-compression",
    "python exon-benchmarks/biopython_scan_fasta.py ${EXON_BENCHMARK_DIR}/data/uniprot_sprot.fasta",
    # FASTA parallel scan benchmarks
    "-n",
    "workers-1",
    "./target/profiling/exon-benchmarks fasta-scan-parallel -p ${EXON_BENCHMARK_DIR}/data/fasta-files -w 1",
    "-n",
    "workers-2",
    "./target/profiling/exon-benchmarks fasta-scan-parallel -p ${EXON_BENCHMARK_DIR}/data/fasta-files -w 2",
    "-n",
    "workers-4",
    "./target/profiling/exon-benchmarks fasta-scan-parallel -p ${EXON_BENCHMARK_DIR}/data/fasta-files -w 4",
    "-n",
    "workers-6",
    "./target/profiling/exon-benchmarks fasta-scan-parallel -p ${EXON_BENCHMARK_DIR}/data/fasta-files -w 6",
    "-n",
    "workers-8",
    "./target/profiling/exon-benchmarks fasta-scan-parallel -p ${EXON_BENCHMARK_DIR}/data/fasta-files -w 8",
    # mzML scan benchmarks
    "-n",
    "exon-gzip",
    "./target/profiling/exon-benchmarks mz-ml-scan -p ${EXON_BENCHMARK_DIR}/data/SALJA0984.mzML.gz -c gzip",
    "-n",
    "exon-no-compression",
    "./target/profiling/exon-benchmarks mz-ml-scan -p ${EXON_BENCHMARK_DIR}/data/SALJA0984.mzML",
    # "-n",
    # "pymzml-gzip",
    # "python exon-benchmarks/pymzml_scan.py ${EXON_BENCHMARK_DIR}/data/SALJA0984.mzML.gz",
    # "-n",
    # "pymzml-no-compression",
    # "python exon-benchmarks/pymzml_scan.py ${EXON_BENCHMARK_DIR}/data/SALJA0984.mzML",
    # "-n",
    # "pyteomics-no-compression",
    # "python exon-benchmarks/pyteomics_scan.py ${EXON_BENCHMARK_DIR}/data/SALJA0984.mzML",
]


@contextmanager
def checkout_commit(repo: Repo, commit: str):
    """Checkout a specific commit and then return to the original commit."""
    original_commit = repo.head.commit
    repo.git.checkout(commit)
    yield
    repo.git.checkout(original_commit)


@dataclass
class Configuration:
    """Configuration for running benchmarks."""

    benchmarks: list[str] = field(default_factory=list)
    exon_executable: str = "exon"
    tags: list[str] = field(default_factory=lambda: ["HEAD"])
    repo_path: str = "."

    def run(self):
        """Run the benchmarks."""
        repo = Repo(self.repo_path)

        for tag in self.tags:
            with checkout_commit(repo, tag):
                print(f"Running benchmarks for tag {tag}")

                templated_command = [
                    Template(cmd).substitute(tag=tag, EXON_BENCHMARK_DIR=PARENT)
                    for cmd in BENCHMARK_CMD
                ]
                subprocess.run(
                    templated_command,
                    check=True,
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run exon benchmarks")

    parser.add_argument(
        "--exon-executable",
        default="exon",
        help="Path to the exon executable",
    )

    parser.add_argument(
        "--tags",
        nargs="+",
        default=["HEAD"],
        help="Tags to run benchmarks on",
    )

    args = parser.parse_args()

    config = Configuration(
        exon_executable=args.exon_executable,
        tags=args.tags,
    )

    config.run()
