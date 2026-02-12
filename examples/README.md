# S3 download example

Spin up minio and create some files:
```bash
# Prepare some files
mkdir data downloads
seq 1 10 | xargs -I{} sh -c 'dd if=/dev/urandom bs=1M count=1 of=data/file{}.bin'

# Run minio and prepare the bucket
docker-compose up -d
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb local/my-bucket
docker exec minio sh -c 'mc cp /local-data/file*.bin local/my-bucket/'
```

Run the download script:
```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_ENDPOINT_URL=http://localhost:9000
export STORAGE_BACKEND_CONFIG=${PWD}/storage_backend.json
uv run -- ./download_s3.py
```

TODO: Add more details about instrumentation and how to verify the results.
