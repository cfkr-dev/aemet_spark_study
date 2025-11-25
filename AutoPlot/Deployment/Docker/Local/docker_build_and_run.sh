#!/bin/bash

# Check if 1 argument is passed
if [ -z "$1" ]; then
  echo "Usage: ./docker_run.sh .env"
  exit 1
fi

ENV_FILE="$1"

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
  echo "File $ENV_FILE not found."
  exit 1
fi

# Read .env file
# Export each variable in the current shell
set -a
source "$ENV_FILE"
set +a

# Print environment variables (for debug)
echo "HOST_EXPOSED_PORT = $HOST_EXPOSED_PORT"
echo "CONTAINER_EXPOSED_PORT = $CONTAINER_EXPOSED_PORT"

echo "HOST_STORAGE_BASE_DIR = $HOST_STORAGE_BASE_DIR"
echo "CONTAINER_STORAGE_BASE_DIR = $CONTAINER_STORAGE_BASE_DIR"

# Set Docker image name
IMAGE_NAME="autoplot:v1.0"

# Build Docker image
echo =======================
echo  BUILDING DOCKER IMAGE
echo =======================
docker build -t "$IMAGE_NAME" .

# Check if Docker build failed
if [ $? -ne 0 ]; then
  echo "DOCKER BUILD ERROR"
  exit $?
fi

# Run Docker container
echo
echo ==========================
echo  RUNNING DOCKER CONTAINER
echo ==========================

docker run --rm -it \
  --env EXPOSED_PORT="$CONTAINER_EXPOSED_PORT" \
  --env STORAGE_BASE_DIR="$CONTAINER_STORAGE_BASE_DIR" \
  -p "$HOST_EXPOSED_PORT:$CONTAINER_EXPOSED_PORT" \
  -v "$HOST_STORAGE_BASE_DIR":"$CONTAINER_STORAGE_BASE_DIR" \
  "$IMAGE_NAME"
