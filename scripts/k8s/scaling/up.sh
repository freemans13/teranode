if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

CONTEXT=$(kubectl config current-context)
if [ "$1" == "help" ]; then
  echo "Usage: $0 [all|eu|m1|us|m2|asia|m3]"
  echo "all: scale up all regions"
  echo "eu or m1: scale up eu region"
  echo "us or m2: scale up us region"
  echo "asia or m3: scale up asia region"
  exit 0
fi

wait() {
  local namespace=$1
  local deployment_name=$2
  local timeout=$3
  echo "Waiting for healthy deployment $deployment_name in namespace $namespace"
  kubectl -n "$namespace" wait --for=condition=available --timeout="${timeout}s" deployment/"$deployment_name"
}

# order is important here, do not change unless you know what you're doing
scale_up() {
  kubectl scale deployment -n m$namespace_suffix blockchain$namespace_suffix --replicas 1
  wait m$namespace_suffix blockchain$namespace_suffix 30
  # kubectl scale deployment -n m$namespace_suffix status$namespace_suffix --replicas 1
  # wait m$namespace_suffix status$namespace_suffix 30
  # asset/blockvalidation/blockassembly need to be scaled up together as they depend on each other :(
  kubectl scale deployment -n m$namespace_suffix asset$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix blockvalidation$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix blockassembly$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix faucet$namespace_suffix --replicas 1
  kubectl scale deployment -n m$namespace_suffix nginx-reverse-proxy --replicas 1
  # wait for all 3 to be ready
  wait m$namespace_suffix asset$namespace_suffix 30
  wait m$namespace_suffix blockvalidation$namespace_suffix 30
  wait m$namespace_suffix blockassembly$namespace_suffix 30
  kubectl scale deployment -n m$namespace_suffix propagation$namespace_suffix --replicas 1
  wait m$namespace_suffix propagation$namespace_suffix 30
  kubectl scale deployment -n m$namespace_suffix p2p$namespace_suffix --replicas 1
  wait m$namespace_suffix p2p$namespace_suffix 30
  echo "Not scaling coinbase and miner as you need to be careful of order when booting the blockchain"
  # todo think of better way to bring up the nodes
#  kubectl scale deployment -n m$namespace_suffix coinbase$namespace_suffix --replicas 1
#  wait m$namespace_suffix coinbase$namespace_suffix 30
#  kubectl scale deployment -n m$namespace_suffix miner$namespace_suffix --replicas 1
}

if [ "$1" == "all" ]; then
  kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
  namespace_suffix=1
  scale_up

  kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
  namespace_suffix=2
  scale_up

  kubectl config use-context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground
  namespace_suffix=3
  scale_up
else
  if [[ "$1" == "eu" || "$1" == "m1" ]]; then
    kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
    namespace_suffix=1
  elif [[ "$1" == "us" || "$1" == "m2" ]]; then
    kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
    namespace_suffix=2
  elif [[ "$1" == "asia" || "$1" == "m3" ]]; then
    kubectl config use-context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground
    namespace_suffix=3
  else
    echo "Auto detecting environment"
    if [[ $(kubectl config current-context) == *"eu-west-1"* ]]; then
      namespace_suffix=1
    elif [[ $(kubectl config current-context) == *"us-east-1"* ]]; then
      namespace_suffix=2
    elif [[ $(kubectl config current-context) == *"ap-south-1"* ]]; then
      namespace_suffix=3
    else
      echo "Unknown context, cannot scale down. Change namespace and try again"
      exit 1
    fi
  fi
  scale_up
fi

kubectl config use-context $CONTEXT
