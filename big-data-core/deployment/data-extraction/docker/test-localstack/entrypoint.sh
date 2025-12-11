#!/bin/sh

set -e

NO_AWS=false

for arg in "$@"; do
  if [ "$arg" = "--no-aws" ]; then
    NO_AWS=true
  fi
done

echo "Waiting for LocalStack S3..."

MAX_RETRIES=30
COUNT=0

while [ "$COUNT" -lt "$MAX_RETRIES" ]; do
  if curl -s "$AWS_S3_ENDPOINT/_localstack/health" | grep '"s3": "available"' > /dev/null; then
    echo "LocalStack ready!"
    break
  fi

  echo "Waiting for LocalStack S3..."
  sleep 2
  COUNT=$((COUNT + 1))
done

if [ "$COUNT" -ge "$MAX_RETRIES" ]; then
  echo "[ERROR] LocalStack S3 did not start after $((MAX_RETRIES * 2)) seconds"
  exit 1
fi

if [ "$NO_AWS" = false ]; then
  aws s3 mb "$STORAGE_PREFIX" --endpoint-url="$AWS_S3_ENDPOINT"

  aws s3 cp \
    "${CONTAINER_STORAGE_PREFIX}${CONTAINER_STORAGE_BASE}/config" \
    "${STORAGE_PREFIX}${STORAGE_BASE}/config" --recursive \
    --endpoint-url="$AWS_S3_ENDPOINT"
fi

java -jar /DataExtraction/app.jar

