CONTEXT=$(kubectl config current-context)
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
  if [ "$second_argument" == "unsafe" ]; then
    # no order is preserved, use this when destroying the env
    kubectl scale deployment -n m$namespace_suffix --all --replicas 0
  else
    # order is important here, do not change unless you know what you're doing
    kubectl scale deployment -n miner$namespace_suffix tx-blaster$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix miner$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix coinbase$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix propagation$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix blockvalidation$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix subtreevalidation$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix blockassembly$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix asset$namespace_suffix --replicas 0
    kubectl scale deployment -n miner$namespace_suffix blockchain$namespace_suffix --replicas 0
  fi
}

if [ "$1" == "all" ]; then
  echo "Scaling down all regions"
  kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
  namespace_suffix=1
  scale_down

  kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
  namespace_suffix=2
  scale_down

  kubectl config use-context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground
  namespace_suffix=3
  scale_down
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
  scale_down
fi

kubectl config use-context $CONTEXT
