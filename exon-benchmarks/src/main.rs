// Copyright 2023 WHERE TRUE Technologies.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{str::FromStr, sync::Arc};

use clap::{Parser, Subcommand};
use datafusion::{
    common::FileCompressionType,
    datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
    prelude::{col, lit, SessionContext},
};
use exon::{
    datasources::{vcf::VCFFormat, ExonFileType, ExonReadOptions},
    new_exon_config, ExonRuntimeEnvExt, ExonSessionExt,
};
use noodles::core::Region;

#[derive(Subcommand)]
enum Commands {
    /// Run a VCF query on a file with a region.
    VCFQuery {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which region to use
        #[arg(short, long)]
        region: String,
    },
    /// Run a BAM query on a file with a region.
    BAMQuery {
        /// which path to use for the BAM file
        #[arg(short, long)]
        path: String,

        /// which region to use
        #[arg(short, long)]
        region: String,
    },
    /// Scan a FASTA file and count the number of non-methionine start codons
    FASTACodonScan {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which compression to use
        #[arg(short, long)]
        compression: Option<FileCompressionType>,
    },
    /// Parallel FASTA Scan
    FASTAScanParallel {
        /// path directory with FASTA files
        #[arg(short, long)]
        path: String,

        /// Number of target partitions
        #[arg(short, long)]
        workers: usize,
    },
    /// Count the number of spectra in a mzML file
    MzMLScan {
        /// which path to use
        #[arg(short, long)]
        path: String,

        /// which compression to use
        #[arg(short, long)]
        compression: Option<FileCompressionType>,
    },
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Turn debugging information on
    #[arg(short, long, action = clap::ArgAction::Count)]
    debug: u8,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match &cli.command {
        Some(Commands::VCFQuery { path, region }) => {
            let path = path.as_str();
            let region: Region = region.parse().unwrap();

            let ctx = SessionContext::new_exon();
            ctx.runtime_env()
                .exon_register_object_store_uri(path)
                .await
                .unwrap();

            let session_state = ctx.state();

            let table_path = ListingTableUrl::parse(path).unwrap();

            let vcf_format = Arc::new(VCFFormat::new(FileCompressionType::GZIP));
            let lo = ListingOptions::new(vcf_format.clone()).with_file_extension("vcf.gz");

            let resolved_schema = lo.infer_schema(&session_state, &table_path).await.unwrap();

            let config = ListingTableConfig::new(table_path)
                .with_listing_options(lo)
                .with_schema(resolved_schema);

            let provider = Arc::new(ListingTable::try_new(config).unwrap());
            ctx.register_table("vcf_file", provider).unwrap();

            let chrom = region.name();
            let start = region.interval().start().unwrap();
            let end = region.interval().end().unwrap();

            let df = ctx
                .sql(format!("SELECT COUNT(*) AS cnt FROM vcf_file WHERE chrom = '{}' and pos BETWEEN {} and {}", chrom, start, end).as_str())
                .await?;

            let batches = df.collect().await?;

            let batch_count = &batches[0];
            eprintln!("Batch count: {:#?}", batch_count);
        }
        Some(Commands::BAMQuery { path, region }) => {
            let path = path.as_str();
            let region = region.as_str();

            let ctx = SessionContext::new_exon();

            let df = ctx.query_bam_file(path, region).await.unwrap();
            let batch_count = df.count().await.unwrap();

            println!("Row count: {batch_count}");
        }
        Some(Commands::FASTACodonScan { path, compression }) => {
            let path = path.as_str();
            let compression = compression.to_owned();

            let ctx = SessionContext::new_exon();

            let df = ctx.read_fasta(path, compression).await.unwrap();

            let count = df
                .filter(col("sequence").ilike(lit("M%")))
                .unwrap()
                .count()
                .await
                .unwrap();

            println!("Count: {count}");
        }
        Some(Commands::FASTAScanParallel { path, workers }) => {
            let exon_config = new_exon_config().with_target_partitions(*workers);
            let ctx = SessionContext::with_config_exon(exon_config);
            let compression = None;
            let df = ctx.read_fasta(path, compression).await.unwrap();

            let count = df
                .filter(col("sequence").ilike(lit("M%")))
                .unwrap()
                .count()
                .await
                .unwrap();

            println!("Count: {count}");
        }
        Some(Commands::MzMLScan { path, compression }) => {
            let path = path.as_str();
            let compression = compression.to_owned();

            let ctx = SessionContext::new_exon();

            let df = ctx.read_mzml(path, compression).await.unwrap();

            let count = df.count().await.unwrap();

            println!("Count: {count}");
        }
        None => {}
    }

    Ok(())
}
