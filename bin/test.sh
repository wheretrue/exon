# A bash script to run the tests, includes setup and teardown.
# Use a trap to ensure the teardown is always run.

set -e

# Setup the trap.
function teardown {
    echo "Tearing down..."
    docker compose down -v
    echo "Teardown completed."
}

trap teardown EXIT

# Setup
echo "Setting up..."

# Start the docker compose stack.
docker compose up -d localstack

# Wait for the stack to start.
sleep 1

# Create the test bucket.
aws --endpoint-url=http://localhost:4566 s3 mb s3://test-bucket

# Upload the test data.
aws --endpoint-url=http://localhost:4566 s3api put-object --bucket test-bucket --key test.fasta --body ./exon/exon-core/test-data/datasources/fasta/test.fasta

# Make the bucket public.
aws --endpoint-url=http://localhost:4566 s3api put-bucket-acl --bucket test-bucket --acl public-read

# Run the tests.
cargo test --test sqllogictests --all-features
