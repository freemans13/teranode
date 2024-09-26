# break on error
set -e

if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

readme() {
  echo "Usage: $0 [all_test|t1|t2|t3|all_main|main1|main2]"
  echo "Testing Environments:"
  echo "\t all_test: scale up all regions in testing environment"
  echo "\t t1: scale up eu region"
  echo "\t t2: scale up us region"
  echo "\t t3: scale up asia region"

  echo "Main Environments:"
  echo "\t all_main: scale up all regions in mainnet environment"
  echo "\t main1: scale up mainnet 1 environment in eu west 1"
  echo "\t main2: scale up mainnet 2 environment in eu west 1"
}
if [ "$1" == "help" ]; then
  readme
  exit 0
fi

# order is important here, do not change unless you know what you're doing
scale_up() {
  local region=$1
  local namespace_suffix=$2
  two_layers_up=$(dirname "$(realpath "$0")")/../../..

  # Create a temporary directory
  TMP_DIR=$(mktemp -d)

  # Copy all *_teranode_v1alpha1_node.yaml files to the temporary directory
  cp ${two_layers_up}/deploy/operator/t${namespace_suffix}_teranode_v1alpha1_node.yaml ${TMP_DIR}

  # Fetch the image name
  SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
  image_name=$(cat "${SCRIPT_DIR}/image_name.tmp")
  rm "${SCRIPT_DIR}/image_name.tmp"

  # Use sed to append after the 'spec:' line
  sed -i.bak '/^spec:$/a\'$'\n''  image: "'"$image_name"'"'$'\n' "${TMP_DIR}/t${namespace_suffix}_teranode_v1alpha1_node.yaml"

  echo "YAML file in $TMP_DIR has been updated with image: $image_name"

  # Use the modified YAML files
  kubectl apply -f ${TMP_DIR}/t${namespace_suffix}_teranode_v1alpha1_node.yaml -n t$namespace_suffix --context arn:aws:eks:$region:434394763103:cluster/aws-ubsv-playground
}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all_test" ]; then
  scale_up "eu-central-1" "1" &
  bg_pids+=($!)
  scale_up "eu-central-1" "2" &
  bg_pids+=($!)
  scale_up "eu-central-1" "3" &
  bg_pids+=($!)
elif [ "$1" == "all_main" ]; then
  scale_up "eu-west-1" "1" &
  bg_pids+=($!)
  scale_up "eu-west-1" "2" &
  bg_pids+=($!)
elif [[ "$1" == "main1" ]]; then
  scale_up "eu-west-1" "1" &
elif [[ "$1" == "main2" ]]; then
  scale_up "eu-west-1" "2" &
elif [[ "$1" == "t1" ]]; then
  scale_up "eu-central-1" "1"
elif [[ "$1" == "t2" ]]; then
  scale_up "eu-central-1" "2"
elif [[ "$1" == "t3" ]]; then
  scale_up "eu-central-1" "3"
else
  echo "You're not specifying the correct environment. Refer to help function for more information.\n"
  readme
fi

# Wait for all background processes to complete
for pid in "${bg_pids[@]}"; do
  wait "$pid" || echo "Process $pid exited with status $?"
done
