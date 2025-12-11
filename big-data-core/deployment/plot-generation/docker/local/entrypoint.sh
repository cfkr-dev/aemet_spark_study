#!/bin/sh

set -e

echo "Waiting for auto-plot..."

MAX_RETRIES=30
COUNT=0

while [ "$COUNT" -lt "$MAX_RETRIES" ]; do
  if curl -s "$AUTO_PLOT_URL_BASE/health" | grep '"status": "ok"' > /dev/null; then
    echo "auto-plot ready!"
    break
  fi

  echo "Waiting for auto-plot..."
  sleep 2
  COUNT=$((COUNT + 1))
done

if [ "$COUNT" -ge "$MAX_RETRIES" ]; then
  echo "[ERROR] auto-plot did not start after $((MAX_RETRIES * 2)) seconds"
  exit 1
fi

java -jar /PlotGeneration/app.jar

