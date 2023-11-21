# scale down everything
bash scale_down.sh

# region postgres m1
psql postgres://m1:m1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/m1 -c "drop table if exists state ; drop table if exists blocks;"
psql postgres://coinbase:coinbase@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
psql postgres://m2:m2@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/m2 -c "drop table if exists state ; drop table if exists blocks;"
psql postgres://coinbase:coinbase@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
psql postgres://m3:m3@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/m3 -c "drop table if exists state ; drop table if exists blocks;"
psql postgres://coinbase:coinbase@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
# endregion

# todo make key dynamic based on user
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem ubuntu@aero.ubsv-store-asia0.ap-northeast-1.ubsv.dev -f "aql -c \"TRUNCATE ubsv-store;"\"
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem ubuntu@aero.ubsv-store-eu0.eu-west-1.ubsv.dev -f "aql -c \"TRUNCATE ubsv-store;"\"
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem ubuntu@aero.ubsv-store-us0.us-east-1.ubsv.dev -f "aql -c \"TRUNCATE ubsv-store;"\"
# scale back up everything
bash scale_up.sh

# scale up tx blaster
kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
