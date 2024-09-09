#if [ -n "$KUBECONFIG" ]; then
#  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
#  exit 1
#fi
set -e
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

  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  kubectl get cluster cluster-sample -n t${namespace_suffix} -o json | jq -r .spec.image > "${SCRIPT_DIR}/image_name.tmp"

  kubectl delete clusters.teranode.bsvblockchain.org -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground --all

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
