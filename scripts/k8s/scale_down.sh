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
kubectl scale deployment -n m1 txmetastore1 --replicas 0

# all in one

kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner1 miner1 --replicas 0
kubectl scale deployment -n miner1 coinbase1 --replicas 0
kubectl scale deployment -n miner1 tx-blaster1 --replicas 0
kubectl scale deployment -n miner2 miner2 --replicas 0
kubectl scale deployment -n miner2 coinbase2 --replicas 0
kubectl scale deployment -n miner2 tx-blaster2 --replicas 0
kubectl scale deployment -n miner3 miner3 --replicas 0
kubectl scale deployment -n miner3 coinbase3 --replicas 0
kubectl scale deployment -n miner3 tx-blaster3 --replicas 0

kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner4 miner4 --replicas 0
kubectl scale deployment -n miner4 coinbase4 --replicas 0
kubectl scale deployment -n miner4 tx-blaster4 --replicas 0
kubectl scale deployment -n miner5 miner5 --replicas 0
kubectl scale deployment -n miner5 coinbase5 --replicas 0
kubectl scale deployment -n miner5 tx-blaster5 --replicas 0
kubectl scale deployment -n miner6 miner6 --replicas 0
kubectl scale deployment -n miner6 coinbase6 --replicas 0
kubectl scale deployment -n miner6 tx-blaster6 --replicas 0

kubectl config use-context arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground
kubectl scale deployment -n miner7 miner7 --replicas 0
kubectl scale deployment -n miner7 coinbase7 --replicas 0
kubectl scale deployment -n miner7 tx-blaster7 --replicas 0
kubectl scale deployment -n miner8 miner8 --replicas 0
kubectl scale deployment -n miner8 coinbase8 --replicas 0
kubectl scale deployment -n miner8 tx-blaster8 --replicas 0
kubectl scale deployment -n miner9 miner9 --replicas 0
kubectl scale deployment -n miner9 coinbase9 --replicas 0
kubectl scale deployment -n miner9 tx-blaster9 --replicas 0
