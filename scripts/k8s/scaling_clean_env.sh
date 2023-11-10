# scale down everything
bash scale_down.sh

# region postgres m1
psql postgresql://m1:m1@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/m1 -c " drop table if exists state ; drop table if exists blocks;"
psql postgresql://coinbase:coinbase@services-db.cipebxcxpkpk.eu-north-1.rds.amazonaws.com:5432/coinbase -c "drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists state; drop table if exists  blocks;"
# endregion

# todo make key dynamic based on user
ssh -i ~/.ssh/joe-ssh-aws-ubsv.pem ubuntu@aero.ubsv-store0.eu-north-1.ubsv.dev -f "aql -c \"TRUNCATE ubsv-store;"\"

# scale back up everything
bash scale_up.sh

# scale up tx blaster
kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
