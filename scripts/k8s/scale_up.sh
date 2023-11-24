wait() {
  local namespace=$1
  local deployment_name=$2
  local timeout=$3

  kubectl -n "$namespace" wait --for=condition=available --timeout="${timeout}s" deployment/"$deployment_name"
}

if [[ $(kubectl config current-context) == *"eu-west-1"* ]]; then
  namespace_suffix=1
elif [[ $(kubectl config current-context) == *"us-east-1"* ]]; then
  namespace_suffix=2
elif [[ $(kubectl config current-context) == *"ap-northeast-1"* ]]; then
  namespace_suffix=3
else
  echo "Unknown context"
  exit 1
fi

# order is important here, do not change unless you know what you're doing
kubectl scale deployment -n m$namespace_suffix blockchain$namespace_suffix --replicas 1
wait m$namespace_suffix blockchain$namespace_suffix 30
#kubectl scale deployment -n m$namespace_suffix utxostore$namespace_suffix --replicas 1
#wait m$namespace_suffix utxostore$namespace_suffix 30
#kubectl scale deployment -n m$namespace_suffix txmetastore$namespace_suffix --replicas 1
#wait m$namespace_suffix txmetastore$namespace_suffix 30
kubectl scale deployment -n m$namespace_suffix asset$namespace_suffix --replicas 1
wait m$namespace_suffix asset$namespace_suffix 30
kubectl scale deployment -n m$namespace_suffix blockassembly$namespace_suffix --replicas 1
wait m$namespace_suffix blockassembly$namespace_suffix 30
kubectl scale deployment -n m$namespace_suffix blockvalidation$namespace_suffix --replicas 1
wait m$namespace_suffix blockvalidation$namespace_suffix 30
kubectl scale deployment -n m$namespace_suffix propagation$namespace_suffix --replicas 1
wait m$namespace_suffix propagation$namespace_suffix 30
kubectl scale deployment -n m$namespace_suffix coinbase$namespace_suffix --replicas 1
wait m$namespace_suffix coinbase$namespace_suffix 30
kubectl scale deployment -n m$namespace_suffix miner$namespace_suffix --replicas 1
