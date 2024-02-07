TXBLASTER=$(kkubectl get deployment -l app=tx-blaster --output 'jsonpath={.items[0].metadata.name}')
kubectl scale deployment $TXBLASTER --replicas 0

MINER=$(kubectl get deployment -l app=miner --output 'jsonpath={.items[0].metadata.name}')
kubectl scale deployment $MINER --replicas 0

kubectl exec -it $(kubectl get pod -l service=postgres -o name) -- psql -h localhost -U postgres -d blockchain -c "drop table if exists state; drop table if exists blocks;"
kubectl exec -it $(kubectl get pod -l service=postgres -o name) -- psql -h localhost -U postgres -d coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists blocks;"
kubectl exec -it $(kubectl get pod -l service=postgres -o name) -- psql -h localhost -U postgres -d txmeta -c "drop table if exists txmeta;"
kubectl exec -it $(kubectl get pod -l service=postgres -o name) -- psql -h localhost -U postgres -d utxostore -c "drop table if exists utxos;"

kubectl scale deployment $MINER --replicas 1