if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

if [ "$1" == "help" ]; then
  echo "Usage: $0 [all|eu|m1|us|m2|asia|m3|m4|m5|m6]"
  echo "all: scale up all regions"
  echo "eu or m1: scale up eu region"
  echo "us or m2: scale up us region"
  echo "asia or m3: scale up asia region"
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

  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockchain$namespace_suffix --replicas 1
  wait_for_scale $region m$namespace_suffix blockchain$namespace_suffix 30
  # kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground status$namespace_suffix --replicas 1
  # wait_for_scale $region m$namespace_suffix status$namespace_suffix 30
  # asset/blockvalidation/blockassembly need to be scaled up together as they depend on each other :(
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground asset$namespace_suffix --replicas 4
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground subtreevalidation$namespace_suffix --replicas 2
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockvalidation$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockassembly$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockpersister$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground faucet$namespace_suffix --replicas 1
  # kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground nginx-reverse-proxy --replicas 1
  # wait for all 3 to be ready
  wait_for_scale $region m$namespace_suffix asset$namespace_suffix 30
  wait_for_scale $region m$namespace_suffix subtreevalidation$namespace_suffix 30
  wait_for_scale $region m$namespace_suffix blockvalidation$namespace_suffix 30
  wait_for_scale $region m$namespace_suffix blockassembly$namespace_suffix 30
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground propagation$namespace_suffix --replicas 28
  wait_for_scale $region m$namespace_suffix propagation$namespace_suffix 30
  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground p2p$namespace_suffix --replicas 1
  wait_for_scale $region m$namespace_suffix p2p$namespace_suffix 30
  echo "Not scaling coinbase and miner as you need to be careful of order when booting the blockchain"
  # todo think of better way to bring up the nodes
  #  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground coinbase$namespace_suffix --replicas 1
  #  wait_for_scale $region m$namespace_suffix coinbase$namespace_suffix 30
  #  kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground miner$namespace_suffix --replicas 1
}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all" ]; then
  scale_up "eu-west-1" "1" &
  bg_pids+=($!)
  scale_up "us-east-1" "2" &
  bg_pids+=($!)
  scale_up "ap-south-1" "3" &
  bg_pids+=($!)
  scale_up "ap-northeast-2" "4" &
  bg_pids+=($!)
  scale_up "ca-central-1" "5" &
  bg_pids+=($!)
  scale_up "us-west-2" "6" &
  bg_pids+=($!)

else
  if [[ "$1" == "eu" || "$1" == "m1" ]]; then
    scale_up "eu-west-1" "1"
  elif [[ "$1" == "us" || "$1" == "m2" ]]; then
    scale_up "us-east-1" "2"
  elif [[ "$1" == "asia" || "$1" == "m3" ]]; then
    scale_up "ap-south-1" "3"
  elif [[ "$1" == "m4" ]]; then
    scale_up "ap-northeast-2" "4"
  elif [[ "$1" == "m5" ]]; then
    scale_up "ca-central-1" "5"
  elif [[ "$1" == "m6" ]]; then
    scale_up "us-west-2" "6"
  else
    echo "Auto detecting environment"
    if [[ $(kubectl config current-context) == *"eu-west-1"* ]]; then
      scale_up "eu-west-1" "1"
    elif [[ $(kubectl config current-context) == *"us-east-1"* ]]; then
      scale_up "us-east-1" "2"
    elif [[ $(kubectl config current-context) == *"ap-south-1"* ]]; then
      scale_up "ap-south-1" "3"
    elif [[ $(kubectl config current-context) == *"ap-northeast-2"* ]]; then
      scale_up "ap-northeast-2" "4"
    elif [[ $(kubectl config current-context) == *"ca-central-1"* ]]; then
      scale_up "ca-central-1" "5"
    elif [[ $(kubectl config current-context) == *"us-west-2"* ]]; then
      scale_up "us-west-2" "6"

    else
      echo "Unknown context, cannot scale down. Change namespace and try again"
      exit 1
    fi
  fi
fi

# Wait for all background processes to complete
for pid in "${bg_pids[@]}"; do
  wait "$pid" || echo "Process $pid exited with status $?"
done
