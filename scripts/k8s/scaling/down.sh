if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

if [ "$1" == "help" ]; then
  echo "Usage: $0 [all|eu|m1|us|m2|asia|m3]"
  echo "all: scale down all regions"
  echo "eu or m1: scale down eu region"
  echo "us or m2: scale down us region"
  echo "asia or m3: scale down asia region"
  exit 0
fi

second_argument="$2"

scale_down() {
  local region=$1
  local namespace_suffix=$2
  
  if [ "$second_argument" == "unsafe" ]; then
    # no order is preserved, use this when destroying the env
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground --all --replicas 0
  else
    # order is important here, do not change unless you know what you're doing
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground tx-blaster$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground miner$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground coinbase$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground propagation$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground p2p$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockvalidation$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockassembly$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground asset$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground status$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground blockchain$namespace_suffix --replicas 0

    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground faucet$namespace_suffix --replicas 0
    kubectl scale deployment -n m$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground nginx-reverse-proxy --replicas 0
  fi
}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all" ]; then
  echo "Scaling down all regions"
  scale_down "eu-west-1" "1" &
  bg_pids+=($!)
  scale_down "us-east-1" "2" &
  bg_pids+=($!)
  scale_down "ap-south-1" "3" &
  bg_pids+=($!)
else
  if [[ "$1" == "eu" || "$1" == "m1" ]]; then
    scale_down "eu-west-1" "1" &
    bg_pids+=($!)
  elif [[ "$1" == "us" || "$1" == "m2" ]]; then
    scale_down "us-east-1" "2" &
    bg_pids+=($!)
  elif [[ "$1" == "asia" || "$1" == "m3" ]]; then
    scale_down "ap-south-1" "3" &
    bg_pids+=($!)
  else
    echo "Auto detecting environment"
    if [[ $(kubectl config current-context) == *"eu-west-1"* ]]; then
      scale_down "eu-west-1" "1"
    elif [[ $(kubectl config current-context) == *"us-east-1"* ]]; then
      scale_down "us-east-1" "2"
    elif [[ $(kubectl config current-context) == *"ap-south-1"* ]]; then
      scale_down "ap-south-1" "3"
    else
      echo "Unknown context, cannot scale down. Change namespace and try again"
      exit 1
    fi
  fi
fi

# Wait for all background processes to finish
for pid in "${bg_pids[@]}"; do
  wait $pid
done

