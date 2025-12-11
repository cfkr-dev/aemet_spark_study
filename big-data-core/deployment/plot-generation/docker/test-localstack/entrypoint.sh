#!/bin/sh

set -e

NO_AWS=false

for arg in "$@"; do
  if [ "$arg" = "--no-aws" ]; then
    NO_AWS=true
  fi
done

MAX_RETRIES=30
SLEEP_TIME=2

echo "Checking services..."

wait_for_auto_plot() {
  COUNT=0
  while [ "$COUNT" -lt "$MAX_RETRIES" ]; do
    if curl -s "$AUTO_PLOT_URL_BASE/health" | grep '"status": "ok"' > /dev/null; then
      echo "[AUTO-PLOT] ready!"
      return 0
    fi
    echo "[AUTO-PLOT] waiting..."
    sleep $SLEEP_TIME
    COUNT=$((COUNT + 1))
  done
  echo "[AUTO-PLOT] [ERROR] did not start after $((MAX_RETRIES * SLEEP_TIME)) seconds"
  exit 1
}

wait_for_localstack() {
  COUNT=0
  while [ "$COUNT" -lt "$MAX_RETRIES" ]; do
    if curl -s "$AWS_S3_ENDPOINT/_localstack/health" | grep '"s3": "available"' > /dev/null; then
      echo "[LOCALSTACK] ready!"
      return 0
    fi
    echo "[LOCALSTACK] waiting..."
    sleep $SLEEP_TIME
    COUNT=$((COUNT + 1))
  done
  echo "[LOCALSTACK] [ERROR] did not start after $((MAX_RETRIES * SLEEP_TIME)) seconds"
  exit 1
}

wait_for_auto_plot &
PID_AUTO_PLOT=$!

wait_for_localstack &
PID_LOCALSTACK=$!

wait $PID_AUTO_PLOT
wait $PID_LOCALSTACK

echo "Both services are ready!"

if [ "$NO_AWS" = false ]; then
  aws s3 mb "$STORAGE_PREFIX" --endpoint-url="$AWS_S3_ENDPOINT"

  aws s3 cp \
    "${CONTAINER_STORAGE_PREFIX}${CONTAINER_STORAGE_BASE}/config" \
    "${STORAGE_PREFIX}${STORAGE_BASE}/config" --recursive \
    --endpoint-url="$AWS_S3_ENDPOINT"

  aws s3 cp \
    "${CONTAINER_STORAGE_PREFIX}${CONTAINER_STORAGE_BASE}/spark" \
    "${STORAGE_PREFIX}${STORAGE_BASE}/spark" --recursive \
    --endpoint-url="$AWS_S3_ENDPOINT"
fi

java -jar /PlotGeneration/app.jar
