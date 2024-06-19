#if [ -n "$KUBECONFIG" ]; then
#  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
#  exit 1
#fi

if [ "$1" == "help" ]; then
  echo "Usage: $0 [all|t1|t2|t3]"
  echo "all: scale down all regions"
  echo "t1: scale down eu region"
  echo "t2: scale down us region"
  echo "t3: scale down asia region"
  exit 0
fi
second_argument="$2"

scale_down() {
  local region=$1
  local namespace_suffix=$2

  if [ "$second_argument" == "unsafe" ]; then
    # no order is preserved, use this when destroying the env
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground --all --replicas 0
  else
    # order is important here, do not change unless you know what you're doing
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground tx-blaster-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground miner-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground coinbase-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground propagation-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground p2p-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockvalidation-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground subtreevalidation-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockassembly-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockpersister-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground asset-t$namespace_suffix --replicas 0
    # kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground status-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockchain-t$namespace_suffix --replicas 0
    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockpersister-t$namespace_suffix --replicas 0

    kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground faucet-t$namespace_suffix --replicas 0
    # kubectl scale deployment -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground nginx-reverse-proxy --replicas 0
  fi
}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all" ]; then
  echo "Scaling down all regions"
  scale_down "eu-central-1" "1" &
  bg_pids+=($!)
  scale_down "eu-central-1" "2" &
  bg_pids+=($!)
  scale_down "eu-central-1" "3" &
  bg_pids+=($!)
else
  if [[ "$1" == "t1" ]]; then
    scale_down "eu-central-1" "1" &
    bg_pids+=($!)
  elif [[ "$1" == "t2" ]]; then
    scale_down "eu-central-1" "2" &
    bg_pids+=($!)
  elif [[ "$1" == "t3" ]]; then
    scale_down "eu-central-1" "3" &
    bg_pids+=($!)
  else
    echo "Unknown context, cannot scale down. Change namespace and try again"
    exit 1
  fi
fi

# Wait for all background processes to finish
for pid in "${bg_pids[@]}"; do
  wait $pid
done
