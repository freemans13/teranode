if [ -n "$KUBECONFIG" ]; then
  echo "KUBECONFIG is set. Please run this script with KUBECONFIG unset."
  exit 1
fi

# scale down everything
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
echo "Scaling down: all"
bash $DIR/down.sh all unsafe


CONTEXT=$(kubectl config current-context)

# Local databases
echo "Postgres cleaning: EU"
kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n postgres postgres-postgresql-0 -- env PGPASSWORD=m1 psql -U m1 -d m1 -c "drop table if exists state ; drop table if exists blocks;"
kubectl exec -it -n postgres postgres-postgresql-0 -- env PGPASSWORD=coinbase psql -U coinbase -d coinbase -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"


echo "Postgres cleaning: US"
kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n postgres postgres-postgresql-0 -- env PGPASSWORD=m2 psql -U m2 -d m2 -c "drop table if exists state ; drop table if exists blocks;"
kubectl exec -it -n postgres postgres-postgresql-0 -- env PGPASSWORD=coinbase psql -U coinbase -d coinbase -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"

# echo "Postgres cleaning: Asia"
kubectl config use-context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n postgres postgres-postgresql-0 -- env PGPASSWORD=m3 psql -U m3 -d m3 -c "drop table if exists state ; drop table if exists blocks;"
kubectl exec -it -n postgres postgres-postgresql-0 -- env PGPASSWORD=coinbase psql -U coinbase -d coinbase -c "drop table if exists state; drop table if exists spendable_utxos; drop table if exists coinbase_utxos; drop table if exists blocks;"


# todo make key dynamic based on user
echo "Aerospike cleaning"
echo "Warning: If aerospike is too large, it might be faster to delete and restart the instances. Talk to the devops team."
echo "Do not truncate a namespace that's too large, it will take hours"

echo "Aerospike cleaning: EU"
kubectl config use-context arn:aws:eks:eu-west-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n aerospike $(kgp -n aerospike -o name| tail -c+5) -- asinfo -h aerospike-0.ubsv.internal -v "truncate-namespace:namespace=ubsv-store;"

echo "Aerospike cleaning: US"
kubectl config use-context arn:aws:eks:us-east-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n aerospike $(kgp -n aerospike -o name| tail -c+5) -- asinfo -h aerospike-0.ubsv.internal -v "truncate-namespace:namespace=ubsv-store;"

# echo "Aerospike cleaning: Asia"
kubectl config use-context arn:aws:eks:ap-south-1:434394763103:cluster/aws-ubsv-playground
kubectl exec -it -n aerospike $(kgp -n aerospike -o name| tail -c+5) -- asinfo -h aerospike-0.ubsv.internal -v "truncate-namespace:namespace=ubsv-store;"

kubectl config use-context $CONTEXT

echo "Scaling back up: all"
# scale back up everything
# bash $DIR/up.sh all
bash $DIR/up.sh m1
bash $DIR/up.sh m2
bash $DIR/up.sh m3

# scale up tx blaster
#kubectl scale deployment -n tx-blaster-service tx-blaster --replicas 1
