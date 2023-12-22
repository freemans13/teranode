# scale down everything
# get relative path for current file
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Scaling down: all"
bash $DIR/down.sh all unsafe

# region delete postgres
# region postgres all in one
echo "Flushing all postgres databases"
psql postgres://miner1:miner1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv1 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner2:miner2@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv2 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner4:miner4@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv4 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner5:miner5@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv5 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner7:miner7@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv7 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner8:miner8@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv8 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
# endregion

# region postgress coinbase
psql postgres://miner1:miner1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase1 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner2:miner2@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase2 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner4:miner4@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase4 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner5:miner5@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase5 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner7:miner7@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase7 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner8:miner8@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase8 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
# endregion

echo "Aerospike cleaning"
CONTEXT=$(kubectl config current-context)
echo "Aerospike cleaning: EU"
kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n aerospike aerocluster-0-0 -c aerospike-server -- asinfo -U admin -P admin123 -v "truncate-namespace:namespace=ubsv-store;"
echo "Aerospike cleaning: US"
kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n aerospike aerocluster-0-0 -c aerospike-server -- asinfo -U admin -P admin123 -v "truncate-namespace:namespace=ubsv-store;"
echo "Aerospike cleaning: Asia"
kubectl config use-context arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n aerospike aerocluster-0-0 -c aerospike-server -- asinfo -U admin -P admin123 -v "truncate-namespace:namespace=ubsv-store;"
kubectl config use-context $CONTEXT

echo "Scaling back up: all"
# scale back up everything
bash $DIR/up.sh all

# scale up tx blaster
#kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
