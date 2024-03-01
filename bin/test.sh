# A bash script to run the tests, includes setup and teardown.
# Use a trap to ensure the teardown is always run.

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

aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/test.fasta s3://test-bucket/test-indexed.fasta
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/test.fasta.fai s3://test-bucket/test-indexed.fasta.fai
aws --endpoint-url=http://localhost:4566 s3 cp ./exon/exon-core/test-data/datasources/fasta-indexed/region.txt s3://test-bucket/region.txt


# Make the bucket public.
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket test-bucket --acl public-read

# Run the tests.
cargo test
