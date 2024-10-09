#!/bin/bash
set -e

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
bg_pids=()

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

  echo "Aerospike cleaning: $region $namespace"
  echo "Warning: If aerospike is too large, it might be faster to delete and restart the instances. Talk to the devops team."
  kubectl exec -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get pod -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | tail -c+5) -- asinfo -h ${namespace}aerospike-0.ubsv.internal -v "truncate-namespace:namespace=txmeta-store;"
  kubectl exec -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground $(kubectl get pod -n aerospike --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -o name | tail -c+5) -- asinfo -h ${namespace}aerospike-0.ubsv.internal -v "truncate-namespace:namespace=utxo-store;"
}

function backup() {
  local region=$1
  local namespace=$2
  local timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  local datePart=$(echo $timestamp | cut -d'T' -f1)
  local filename="${timestamp}-${region}-${namespace}.dump"
  local s3Path="s3://ubsv-blockchain-backups/${datePart}/${filename}"

  echo "Postgres backup: $region $namespace > $s3Path"
  kubectl exec -n postgres postgres-postgresql-0 --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -- env PGPASSWORD=$namespace pg_dump -U $namespace $namespace >/tmp/$filename
  aws s3 cp /tmp/$filename $s3Path
  rm /tmp/$filename
}

function truncate() {
  local region=$1
  local namespace=$2
  local ns_suffix=$(echo $namespace | tail -c+2)

  echo "Postgres cleaning: $region $namespace"
  kubectl exec -n postgres postgres-postgresql-0 --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -- env PGPASSWORD=$namespace psql -U $namespace -d $namespace -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks;" >/dev/null
  kubectl exec -n postgres postgres-postgresql-0 --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground -- env PGPASSWORD=coinbase${ns_suffix} psql -U coinbase${ns_suffix} -d coinbase${ns_suffix} -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;" >/dev/null
}

function clear_kafka() {
  local namespace=$1
  make -C $DIR/../../../cmd/kafkatool
  echo "SETTINGS_CONTEXT=testing.${namespace} $DIR/../../../cmd/kafkatool/kafkatool"
}

function clean_env() {
  local region=$1
  local namespace=$2
  clean $region $namespace &
  bg_pids+=($!)

  truncate $region $namespace &
  bg_pids+=($!)

  clear_kafka $namespace &
  bg_pids+=($!)
}

function clean_test_envs() {
  echo "Scaling down: all test environments"
  bash $DIR/down.sh all_test unsafe

  for i in 1 2 3; do
    clean_env "eu-central-1" "t$i"
  done

  for pid in "${bg_pids[@]}"; do
    wait "$pid" || echo "Process $pid exited with status $?"
  done

  echo "Scaling back up: all test environments"
  bash $DIR/up.sh all_test
}

function clean_main_envs() {
  echo "Scaling down: all mainnet environments"
  bash $DIR/down.sh all_main unsafe

  for i in 1 2; do
    clean_env "eu-west-1" "main$i"
  done

  for pid in "${bg_pids[@]}"; do
    wait "$pid" || echo "Process $pid exited with status $?"
  done

  echo "Scaling back up: all mainnet environments"
  bash $DIR/up.sh all_main
}

function clean_individual_env() {
  local env_type=$1
  local env_number=$2

  if [[ $env_type == "t" ]]; then
    echo "Cleaning test environment t${env_number}"
    clean_env "eu-central-1" "t${env_number}"
    bash $DIR/up.sh "t${env_number}"
  elif [[ $env_type == "main" ]]; then
    echo "Cleaning mainnet environment main${env_number}"
    clean_env "eu-west-1" "main${env_number}"
    bash $DIR/up.sh "main${env_number}"
  else
    echo "Invalid environment specified. Use t1, t2, t3, main1, or main2."
    exit 1
  fi

  for pid in "${bg_pids[@]}"; do
    wait "$pid" || echo "Process $pid exited with status $?"
  done
}

# Parse input arguments and call appropriate functions
if [ "$1" == "all_test" ]; then
  clean_test_envs
elif [ "$1" == "all_main" ]; then
  clean_main_envs
elif [[ "$1" =~ ^t[1-3]$ ]]; then
  clean_individual_env "t" "${1:1}"
elif [[ "$1" =~ ^main[1-2]$ ]]; then
  clean_individual_env "main" "${1:4}"
else
  echo "Invalid argument. Usage: $0 [all_test|all_main|t1|t2|t3|main1|main2]"
  exit 1
fi
