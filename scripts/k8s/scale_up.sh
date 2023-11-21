wait() {
  local namespace=$1
  local deployment_name=$2
  local timeout=$3

  kubectl -n "$namespace" wait --for=condition=available --timeout="${timeout}s" deployment/"$deployment_name"
}

# order is important here, do not change unless you know what you're doing
kubectl scale deployment -n m1 blockchain1 --replicas 1
wait m1 blockchain1 30
#kubectl scale deployment -n m1 utxostore1 --replicas 1
#wait m1 utxostore1 30
#kubectl scale deployment -n m1 txmetastore1 --replicas 1
#wait m1 txmetastore1 30
kubectl scale deployment -n m1 asset1 --replicas 1
wait m1 asset1 30
kubectl scale deployment -n m1 blockassembly1 --replicas 1
wait m1 blockassembly1 30
kubectl scale deployment -n m1 blockvalidation1 --replicas 1
wait m1 blockvalidation1 30
kubectl scale deployment -n m1 validation1 --replicas 1
wait m1 validation1 30
kubectl scale deployment -n m1 propagation1 --replicas 1
wait m1 propagation1 30
kubectl scale deployment -n m1 coinbase1 --replicas 1
wait m1 coinbase1 30
kubectl scale deployment -n m1 miner1 --replicas 1


# all in one
# kubectl scale deployment -n miner1 miner1 --replicas 1
# kubectl scale deployment -n miner2 miner2 --replicas 1
# kubectl scale deployment -n miner3 miner3 --replicas 1
