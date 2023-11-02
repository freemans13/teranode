# scale down everything
bash scale_down.sh

# delete
psql postgres://miner1:miner1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv1 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner2:miner2@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv2 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner3:miner3@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv3 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner4:miner4@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv4 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner5:miner5@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv5 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner6:miner6@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv6 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner7:miner7@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv7 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner8:miner8@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv8 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"
psql postgres://miner9:miner9@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv9 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists utxos; drop table if exists blocks;"

psql postgres://miner1:miner1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase1 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner2:miner2@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase2 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner3:miner3@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase3 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner4:miner4@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase4 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner5:miner5@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase5 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner6:miner6@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase6 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner7:miner7@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase7 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner8:miner8@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase8 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"
psql postgres://miner9:miner9@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase9 -c "drop table if exists state; drop table if exists coinbase_utxos; drop table if exists spendable_utxos; drop table if exists blocks;"


psql postgresql://m1:m1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/m1 -c " drop table if exists state ; drop table if exists blocks;"
psql postgresql://coinbase:coinbase@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
# truncate
psql postgresql://m1:m1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/m1 -c "truncate table state CASCADE; truncate table blocks CASCADE; truncate table coinbase_utxos CASCADE;"
psql postgresql://coinbase:coinbase@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/coinbase -c "truncate table state CASCADE; truncate table blocks CASCADE; truncate table coinbase_utxos CASCADE;"

# region clean redis
clusters=("arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground"
          "arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground"
          "arn:aws:eks:ap-northeast-1:434394763103:cluster/aws-ubsv-playground")
offset=0

for cluster in "${clusters[@]}"; do
  for i in {1..3}; do
    kubectl config use-context $cluster
    index=$((offset + i))  # Calculate the index
    (kubectl port-forward -n "redis-miner${index}" "redis-store-${index}-redis-cluster-0" 6379:6379) &
    pid=$!
    sleep 2
    redis-cli -h localhost -a TfocK5PCg7 -c -n "${clusters[index]}" FLUSHALL
    kill -9 $pid
    offset=$((offset + 1))
  done
done
# endregion

# todo make key dynamic based on user
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem  ubuntu@aero.ubsv-store0.eu-north-1.ubsv.dev -f "aql -c \"TRUNCATE ubsv-store;"\"

# scale back up everything
bash scale_up.sh

# scale up tx blaster
kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
