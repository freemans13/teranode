#!/bin/bash

# Define namespaces
namespaces=("t1" "t2" "t3")

# Loop through each namespace
for namespace in "${namespaces[@]}"; do
  # Get the pod name dynamically that starts with 'blockchain-' in the current namespace
  POD_NAME=$(kubectl get pods -n "$namespace" --no-headers -o custom-columns=":metadata.name" | grep '^blockchain-')

  # Check if the pod name was found
  if [ -z "$POD_NAME" ]; then
    echo "No pod starting with 'blockchain-' found in namespace $namespace."
  else
    echo "Found pod $POD_NAME in namespace $namespace. Running command..."

    # Run the command using the found pod name
    kubectl exec -t -i -n "$namespace" "$POD_NAME" -- grpcurl -plaintext localhost:8087 blockchain_api.BlockchainAPI.Run
  fi
done