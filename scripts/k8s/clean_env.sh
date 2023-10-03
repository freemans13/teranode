# scale down everything
bash scale_down.sh

# delete
psql postgres://miner1:miner1@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv1 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner2:miner2@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv2 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner3:miner3@miners-db.czklemh7vdzk.eu-west-1.rds.amazonaws.com:5432/ubsv3 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner4:miner4@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv4 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner5:miner5@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv5 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner6:miner6@miners-db.cwhjir53bktc.us-east-1.rds.amazonaws.com:5432/ubsv6 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner7:miner7@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv7 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner8:miner8@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv8 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"
psql postgres://miner9:miner9@miners-db.cfhjsqgwu9jw.ap-northeast-1.rds.amazonaws.com:5432/ubsv9 -c "drop table if exists state; drop table if exists utxos; drop table if exists txmeta; drop table if exists blocks; drop table if exists coinbase_utxos;"

psql postgresql://m1:m1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/m1 -c " drop table if exists state ; drop table if exists blocks;"
psql postgresql://coinbase:coinbase@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/coinbase -c "drop table coinbase_utxos; drop table state; drop table blocks;"
# truncate
psql postgresql://m1:m1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/m1 -c "truncate table state CASCADE; truncate table blocks CASCADE; truncate table coinbase_utxos CASCADE;"
psql postgresql://coinbase:coinbase@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/coinbase -c "truncate table state CASCADE; truncate table blocks CASCADE; truncate table coinbase_utxos CASCADE;"

# todo make key dynamic based on user
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem ubuntu@aero.utxo-store3.eu-north-1.ubsv.dev -f "aql -c \"TRUNCATE utxostore\""
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem ubuntu@aero.tx-status-store4.eu-north-1.ubsv.dev -f "aql -c \"TRUNCATE txstatus_store\""

# scale back up everything
bash scale_up.sh

# scale up tx blaster
kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
