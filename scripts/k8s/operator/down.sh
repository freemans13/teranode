#if [ -n "$KUBECONFIG" ]; then
#  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
#  exit 1
#fi
set -e
readme() {
  echo "Usage: $0 [all_test|t1|t2|t3|all_main|main1|main2]"
  echo "Testing Environments:"
  echo "\t all_test: scale down all regions in testing environment"
  echo "\t t1: scale down eu region"
  echo "\t t2: scale down us region"
  echo "\t t3: scale down asia region"

  echo "Main Environments:"
  echo "\t all_main: scale down all regions in mainnet environment"
  echo "\t main1: scale down mainnet 1 environment in eu west 1"
  echo "\t main2: scale down mainnet 2 environment in eu west 1"

}
if [ "$1" == "help" ]; then
  readme
  exit 0
fi
second_argument="$2"

scale_down() {
  local region=$1
  local namespace_suffix=$2

  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  kubectl get cluster cluster-sample -n t${namespace_suffix} -o json | jq -r .spec.image >"${SCRIPT_DIR}/image_name.tmp"

  kubectl delete clusters.teranode.bsvblockchain.org -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/teranet --all

}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all_test" ]; then
  scale_down "eu-central-1" "1" &
  bg_pids+=($!)
  scale_down "eu-central-1" "2" &
  bg_pids+=($!)
  scale_down "eu-central-1" "3" &
  bg_pids+=($!)
elif [ "$1" == "all_main" ]; then
  scale_down "eu-west-1" "1" &
  bg_pids+=($!)
  scale_down "eu-west-1" "2" &
  bg_pids+=($!)
elif [[ "$1" == "main1" ]]; then
  scale_down "eu-west-1" "1" &
elif [[ "$1" == "main2" ]]; then
  scale_down "eu-west-1" "2" &
elif [[ "$1" == "t1" ]]; then
  scale_down "eu-central-1" "1"
elif [[ "$1" == "t2" ]]; then
  scale_down "eu-central-1" "2"
elif [[ "$1" == "t3" ]]; then
  scale_down "eu-central-1" "3"
else
  echo "You're not specifying the correct environment. Refer to help function for more information.\n"
  readme
  exit 1
fi

# Wait for all background processes to finish
for pid in "${bg_pids[@]}"; do
  wait $pid
done
