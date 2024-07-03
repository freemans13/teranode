#!/bin/bash

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

# Check if kubectl is installed
if ! command -v kubectl &>/dev/null; then
  echo "kubectl could not be found, please install it to continue (brew install kubectl)."
  exit 1
fi

# Check if aws is installed
if ! command -v aws &>/dev/null; then
  echo "aws cli could not be found, please install it to continue (brew install awscli)."
  exit 1
fi

# Array to hold PIDs of background processes
declare -g -a bg_pids

# Function to handle SIGINT
function kill_background_processes() {
  echo "Caught SIGINT, stopping background processes..."
  for pid in "${bg_pids[@]}"; do
    kill -SIGINT "$pid" 2>/dev/null
  done
  exit 1
}

# Setup trap for SIGINT
trap 'kill_background_processes' SIGINT

function clean() {
  local region=$1
  local namespace=$2

  echo "Aerospike cleaning: $region"
  echo "Aerospike cleaning"
  echo "Warning: If aerospike is too large, it might be faster to delete and restart the instances. Talk to the devops team."
  echo "Do not truncate a namespace that's too large, it will take hours"

  kubectl exec -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get pod -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | tail -c+5) -- asinfo -h ${namespace}aerospike-0.ubsv.internal -v "truncate-namespace:namespace=txmeta-store;"
  kubectl exec -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get pod -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | tail -c+5) -- asinfo -h ${namespace}aerospike-0.ubsv.internal -v "truncate-namespace:namespace=utxo-store;"
  # echo "Clearing Lustre: $region"
  # kubectl scale deployment -n $namespace --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get deployment --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | grep blockchain | tail -c+17) --replicas 1
  # kubectl scale deployment -n $namespace --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get deployment --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | grep asset | tail -c+17) --replicas 1
  # kubectl exec -n $namespace --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get pod --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | grep asset | tail -c+5) -- find /data/subtreestore -type f -delete
}

function backup() {
  local region=$1
  local namespace=$2
  local timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)    # Generate timestamp in UTC with -u option
  local datePart=$(echo $timestamp | cut -d'T' -f1) # Extract the date part (yyyy-mm-dd)
  local filename="${timestamp}-${region}-${namespace}.dump"
  local s3Path="s3://ubsv-blockchain-backups/${datePart}/${filename}" # Include datePart in the path

  echo "Postgres backup: $region $namespace > $s3Path"
  kubectl exec -n postgres postgres-postgresql-0 --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -- env PGPASSWORD=$namespace pg_dump -U $namespace $namespace >/tmp/$filename
  aws s3 cp /tmp/$filename $s3Path
  rm /tmp/$filename
}

function truncate() {
  local region=$1
  local namespace=$2
  # get the last part of the namespace
  local ns_suffix=$(echo $namespace | tail -c+2)
  backup $region $namespace

  echo "Postgres cleaning: $region $namespace"
  kubectl exec -n postgres postgres-postgresql-0 --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -- env PGPASSWORD=$namespace psql -U $namespace -d $namespace -c "drop table if exists state ; drop table if exists blocks;" >/dev/null
  kubectl exec -n postgres postgres-postgresql-0 --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -- env PGPASSWORD=coinbase${ns_suffix} psql -U coinbase${ns_suffix} -d coinbase${ns_suffix} -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;" >/dev/null
}

function clear_kafka() {
  local namespace=$1

  # Make the kafkatool if necessary...
  make -C $DIR/../../../cmd/kafkatool

  echo "SETTINGS_CONTEXT=scaling.${namespace} $DIR/../../../cmd/kafkatool/kafkatool"
  # SETTINGS_CONTEXT=scaling.${namespace} $DIR/../../../cmd/kafkatool/kafkatool
}


# scale down everything
echo "Scaling down: all"
bash $DIR/down.sh all unsafe

for i in 1 2 3; do
    clean "eu-central-1" "t$i" &
    bg_pids+=($!)

    # Local databases
    truncate "eu-central-1" "t$i" &
    bg_pids+=($!)

    # Kafka
    clear_kafka "t$i" &
    bg_pids+=($!)

  done

# Wait for all background processes to complete
for pid in "${bg_pids[@]}"; do
  wait "$pid" || echo "Process $pid exited with status $?"
done

echo "Scaling back up: all"
# scale back up everything
bash $DIR/up.sh all

# scale up tx blaster
#kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
