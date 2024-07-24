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

wait_for_scale() {
  local region=$1
  local namespace=$2
  local deployment_name=$3
  local timeout=$4

  echo "Waiting for healthy deployment $deployment_name in namespace $namespace"
  kubectl -n "$namespace" --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground wait --for=condition=available --timeout="${timeout}s" deployment/"$deployment_name"
}

# order is important here, do not change unless you know what you're doing
scale_up() {
  local region=$1
  local namespace_suffix=$2

  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockchain-t$namespace_suffix --replicas 1
  wait_for_scale $region t$namespace_suffix blockchain-t$namespace_suffix 30
  # kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground status-t$namespace_suffix --replicas 1
  # wait_for_scale $region t$namespace_suffix status$namespace_suffix 30
  # asset/blockvalidation/blockassembly need to be scaled up together as they depend on each other :(
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground asset-t$namespace_suffix --replicas 1
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground subtreevalidation-t$namespace_suffix --replicas 1
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockvalidation-t$namespace_suffix --replicas 1
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockassembly-t$namespace_suffix --replicas 1
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockpersister-t$namespace_suffix --replicas 1
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground faucet-t$namespace_suffix --replicas 1
  # kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground nginx-reverse-proxy --replicas 1
  # wait for all 3 to be ready
  wait_for_scale $region t$namespace_suffix asset-t$namespace_suffix 30
  wait_for_scale $region t$namespace_suffix subtreevalidation-t$namespace_suffix 30
  wait_for_scale $region t$namespace_suffix blockvalidation-t$namespace_suffix 30
  wait_for_scale $region t$namespace_suffix blockassembly-t$namespace_suffix 30
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground propagation-t$namespace_suffix --replicas 1
  wait_for_scale $region t$namespace_suffix propagation-t$namespace_suffix 30
  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground p2p-t$namespace_suffix --replicas 1
  wait_for_scale $region t$namespace_suffix p2p-t$namespace_suffix 30
  echo "Not scaling coinbase and miner as you need to be careful of order when booting the blockchain"
  # todo think of better way to bring up the nodes
  #  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground coinbase-t$namespace_suffix --replicas 1
  #  wait_for_scale $region t$namespace_suffix coinbase$namespace_suffix 30
  #  kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground miner-t$namespace_suffix --replicas 1
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
