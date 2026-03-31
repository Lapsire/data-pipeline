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

# Sleep until kafka is full
WAIT_TIME=${STRESS_TEST_WAIT_TIME:-30}
echo "Waiting for ${WAIT_TIME}"

sleep $WAIT_TIME

# Letting spark do the job
echo "Free spark"
docker unpause spark-submitter