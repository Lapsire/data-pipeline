#!/bin/bash

# Getenv
if [ -f .env ]; then
    export $(grep -v '^#' .env | sed 's/#.*//' | xargs)
fi

# Check if stress test enabled in .env
if [ "$STRESS_TEST_ENABLED" != "true" ]; then
    echo "STRESS TEST FLAG NOT ACTIVE"
    echo "PUT STRESS_TEST_ENABLED=true if you want to run stress test."
    exit 0
fi

echo "STARTING STRESS TEST..."

# Pause container
docker pause spark-submitter

# Fonction appelée lors du Ctrl+C
cleanup() {
    echo ""
    echo "Ctrl+C detected — unpausing spark-submitter..."
    docker unpause spark-submitter
    exit 0
}

# Trap Ctrl+C
trap cleanup SIGINT

echo "Container paused. Waiting indefinitely... (Press Ctrl+C to continue)"

# Infinite loop waiting the SIGINT
while true; do
    sleep 1
done