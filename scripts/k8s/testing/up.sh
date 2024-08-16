# break on error
set -e

if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

if [ "$1" == "help" ]; then
  echo "Usage: $0 [all|t1|t2|t3]"
  echo "all: scale up all regions"
  echo "t1: scale up eu region"
  echo "t2: scale up us region"
  echo "t3: scale up asia region"
  exit 0
fi

# order is important here, do not change unless you know what you're doing
scale_up() {
  local region=$1
  local namespace_suffix=$2
  two_layers_up=$(dirname "$(realpath "$0")")/../../..
  kubectl apply -f $two_layers_up/deploy/operator/t${namespace_suffix}_teranode_v1alpha1_node.yaml -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground
}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all" ]; then
  scale_up "eu-central-1" "1" &
  bg_pids+=($!)
  scale_up "eu-central-1" "2" &
  bg_pids+=($!)
  scale_up "eu-central-1" "3" &
  bg_pids+=($!)
else
  if [[ "$1" == "t1" ]]; then
    scale_up "eu-central-1" "1"
  elif [[ "$1" == "t" ]]; then
    scale_up "eu-central-1" "2"
  elif [[ "$1" == "t3" ]]; then
    scale_up "eu-central-1" "3"
  else
    echo "T1, T2, T3 are sharing the same environment. Please specify one of them."
  fi
fi

# Wait for all background processes to complete
for pid in "${bg_pids[@]}"; do
  wait "$pid" || echo "Process $pid exited with status $?"
done
