# scale down everything
bash scale_down.sh

psql postgresql://coinbase:coinbase@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/coinbase -c "drop table coinbase_utxos; drop table state; drop table blocks;"
psql postgresql://miner1:miner1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/ubsv1 -c " drop table state; drop table utxos; drop table txmeta;drop table blocks;"
psql postgresql://miner2:miner2@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/ubsv2 -c " drop table state; drop table utxos; drop table txmeta;drop table blocks;"
psql postgresql://miner3:miner3@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/ubsv3 -c " drop table state; drop table utxos; drop table txmeta;drop table blocks;"
psql postgresql://m1:m1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/m1 -c " drop table if exists state ; drop table if exists blocks;"

# todo make key dynamic based on user
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem  ubuntu@aero.utxo0.ubsv.dev
aql "TRUNCATE utxostore" # todo fix how to run this dynamically

ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem  ubuntu@aero.tx0.ubsv.dev
aql "TRUNCATE txstatus_store" # todo fix how to run this dynamically

# scale back up everything
bash scale_up.sh


kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
