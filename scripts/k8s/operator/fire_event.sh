# break on error
set -e

if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

if [ "$1" == "help" ]; then
  echo "Usage: $0 [all|t1|t2|t3] [event]"
  echo "all: run all regions"
  echo "t1: run eu region"
  echo "t2: run us region"
  echo "t3: run asia region"
  echo "event: the event to pass (e.g., RUN, STOP, etc.)"
  exit 0
fi

# Check if the event is passed
if [ -z "$2" ]; then
  echo "Event not specified. Please provide an event to pass."
  exit 1
fi

# Convert the event to uppercase
EVENT=$(echo "$2" | tr '[:lower:]' '[:upper:]')

# order is important here, do not change unless you know what you're doing
fire_event() {
  local region=$1
  local namespace_suffix=$2

  # Get the name of the pod
  POD_NAME=$(kubectl get pods -n "t${namespace_suffix}" -l "app=blockchain" -o jsonpath='{.items[0].metadata.name}')

  # Check if the POD_NAME is not empty
  if [[ -z "${POD_NAME}" ]]; then
    echo "No pod found with the label selector: app=blockchain in namespace: t${namespace_suffix}"
    exit 1
  fi

  kubectl exec -i -n t${namespace_suffix} ${POD_NAME} -- grpcurl -plaintext -d "{\"event\": \"$EVENT\"}" localhost:8087 blockchain_api.BlockchainAPI/SendFSMEvent
}

# Array to hold PIDs of background processes
bg_pids=()

if [ "$1" == "all" ]; then
  fire_event "eu-central-1" "1" &
  bg_pids+=($!)
  fire_event "eu-central-1" "2" &
  bg_pids+=($!)
  fire_event "eu-central-1" "3" &
  bg_pids+=($!)
else
  if [[ "$1" == "t1" ]]; then
    fire_event "eu-central-1" "1"
  elif [[ "$1" == "t2" ]]; then
    fire_event "eu-central-1" "2"
  elif [[ "$1" == "t3" ]]; then
    fire_event "eu-central-1" "3"
  else
    echo "T1, T2, T3 are sharing the same environment. Please specify one of them."
  fi
fi

# Wait for all background processes to complete
for pid in "${bg_pids[@]}"; do
  wait "$pid" || echo "Process $pid exited with status $?"
done