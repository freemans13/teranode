# scale down everything
# get relative path for current file
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
# echo "Scaling down: all"
# bash $DIR/down.sh all unsafe

# region delete postgres
kubectl exec -it $(kubectl get pod -l service=miner -o name) -- psql -h localhost -U postgres -d blockchain -c "drop table if exists state; drop table if exists blocks;"
kubectl exec -it $(kubectl get pod -l service=miner -o name) -- psql -h localhost -U postgres -d coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists blocks;"
kubectl exec -it $(kubectl get pod -l service=miner -o name) -- psql -h localhost -U postgres -d txmeta -c "drop table if exists txmeta;"
kubectl exec -it $(kubectl get pod -l service=miner -o name) -- psql -h localhost -U postgres -d utxostore -c "drop table if exists utxos;"

# 
# echo "Scaling back up: all"
# scale back up everything
# bash $DIR/up.sh all

# scale up tx blaster
#kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
