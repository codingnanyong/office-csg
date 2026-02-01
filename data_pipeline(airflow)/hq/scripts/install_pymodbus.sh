#!/bin/bash

# Container List Initialization
EXTRA_CONTAINERS=("airflow-webserver" "airflow-scheduler")

# airflow-worker-1 ~ 10 Add
for i in $(seq 1 10); do
  EXTRA_CONTAINERS+=("airflow-airflow-worker-$i")
done

# Install pymodbus in each container
for CONTAINER in "${EXTRA_CONTAINERS[@]}"; do
  echo "📦 Installing pymodbus in $CONTAINER ..."
  docker exec -it "$CONTAINER" bash -c "pip install pyModbus==0.2.0"
  echo "✅ Done with $CONTAINER"
  echo "-----------------------------"
done
