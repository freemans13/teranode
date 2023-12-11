# scale down everything
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
echo "Scaling down: all"
bash $DIR/scaling_down.sh all unsafe

echo "Flushing all postgres databases"
# region postgres m1
psql postgres://m1:m1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/m1 -c "drop table if exists state ; drop table if exists blocks;"
psql postgres://coinbase:coinbase@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
psql postgres://m2:m2@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/m2 -c "drop table if exists state ; drop table if exists blocks;"
psql postgres://coinbase:coinbase@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
psql postgres://m3:m3@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/m3 -c "drop table if exists state ; drop table if exists blocks;"
psql postgres://coinbase:coinbase@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
# endregion

# todo make key dynamic based on user
echo "Aerospike cleaning"
CONTEXT=$(kubectl config current-context)
echo "Aerospike cleaning: EU"
kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
keti -n aerospike aerocluster-0-0 -c aerospike-server -- asinfo -U admin -P admin123 -v "truncate-namespace:namespace=ubsv-store;"
echo "Aerospike cleaning: US"
kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
keti -n aerospike aerocluster-0-0 -c aerospike-server -- asinfo -U admin -P admin123 -v "truncate-namespace:namespace=ubsv-store;"
echo "Aerospike cleaning: Asia"
kubectl config use-context arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground
keti -n aerospike aerocluster-0-0 -c aerospike-server -- asinfo -U admin -P admin123 -v "truncate-namespace:namespace=ubsv-store;"
kubectl config use-context $CONTEXT

echo "Scaling back up: all"
# scale back up everything
bash $DIR/scaling_up.sh all

# scale up tx blaster
kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
