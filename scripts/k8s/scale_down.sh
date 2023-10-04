# tx blaster
kubectl scale deployment -n m1 tx-blaster1 --replicas 0

# m1
kubectl scale deployment -n m1 blockchain1 --replicas 0
kubectl scale deployment -n m1 coinbase1 --replicas 0
kubectl scale deployment -n m1 blockassembly1 --replicas 0
kubectl scale deployment -n m1 blob1 --replicas 0
kubectl scale deployment -n m1 blockvalidation1 --replicas 0
kubectl scale deployment -n m1 validation1 --replicas 0
kubectl scale deployment -n m1 propagation1 --replicas 0
kubectl scale deployment -n m1 miner1 --replicas 0
kubectl scale deployment -n m1 utxostore1 --replicas 0

# all in one
kubectl scale deployment -n miner1 miner1 --replicas 0
kubectl scale deployment -n miner2 miner2 --replicas 0
kubectl scale deployment -n miner3 miner3 --replicas 0
