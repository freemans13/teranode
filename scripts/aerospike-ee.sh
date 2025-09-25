
#!/bin/bash

# Use DATADIR environment variable if set, otherwise default to ./data
DATADIR="${DATADIR:-./data}"

mkdir -p ${DATADIR}/aerospike

DATA_PATH=$(realpath $DATADIR)
export DATA_PATH

cd deploy/docker/aerospike || exit
docker compose -f docker-compose-ee.yml up -d

echo "Aerospike enterprise edition evaluation mode started with persistent data in '${DATA_PATH}/aerospike"
echo "Connect with aerospike://localhost:3000"
