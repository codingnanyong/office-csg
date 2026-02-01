#!/bin/bash

# Container List Initialization
EXTRA_CONTAINERS=("airflow-webserver" "airflow-scheduler")

# airflow-worker-1 ~ 3 Add
for i in $(seq 1 5); do
  EXTRA_CONTAINERS+=("airflow-airflow-worker-$i")
done

# Install libaio1 in each container
for CONTAINER in "${EXTRA_CONTAINERS[@]}"; do
  echo "📦 Installing libaio1 in $CONTAINER ..."
  docker exec -u root -it "$CONTAINER" bash -c "apt-get update && apt-get install -y libaio1"
  echo "✅ Done with $CONTAINER"
  echo "-----------------------------"
done
