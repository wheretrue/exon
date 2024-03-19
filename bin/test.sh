#!/usr/bin/env bash

set -e

# Setup the trap.
function teardown {
    echo "Tearing down..."
    docker compose down -v
    echo "Teardown completed."
}

# check docker and aws cli are installed
if ! command -v docker &> /dev/null
then
    echo "docker could not be found"
    exit
fi

if ! command -v aws &> /dev/null
then
    echo "aws cli could not be found"
    exit
fi


# Setup
echo "Setting up..."

trap teardown EXIT

# Start the docker compose stack.
docker compose up -d localstack

# Wait for the stack to start.
sleep 2

# Create the test bucket.
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket

# Upload the test data
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta/test.fasta s3://test-bucket/test.fasta
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta/test.fasta s3://test-bucket/test.fa

aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/test.fasta s3://test-bucket/test-indexed.fasta
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/test.fasta.gz s3://test-bucket/test-indexed.fasta.gz
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/test.fasta.gz.fai s3://test-bucket/test-indexed.fasta.gz.fai
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/test.fasta.fai s3://test-bucket/test-indexed.fasta.fai
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/region.txt s3://test-bucket/region.txt

# cram data
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/cram/1404_index_multislice.cram s3://test-bucket/1404_index_multislice.cram
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/cram/1404_index_multislice.cram.crai s3://test-bucket/1404_index_multislice.cram.crai
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/cram/ce.fa s3://test-bucket/ce.fa
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/cram/ce.fa.fai s3://test-bucket/ce.fa.fai
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/cram/test_input_1_a.cram s3://test-bucket/test_input_1_a.cram
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/cram/0500_mapped.cram s3://test-bucket/0500_mapped.cram

# Make the bucket public.
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket test-bucket --acl public-read

# Run the tests.
cargo test
