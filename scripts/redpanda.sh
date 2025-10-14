#!/bin/sh

# Create network only if it doesn't exist
if ! docker network ls | grep -q app-tier; then
    docker network create app-tier --driver bridge
fi

# Start Redpanda container
echo "Starting Redpanda on port 9092..."
docker run --rm -d -p 9092:9092 -p 19092:19092 -p 9644:9644 --name redpanda-server \
    --network app-tier \
    -m 256m \
    redpandadata/redpanda:latest \
    redpanda start \
    --smp 1 \
    --overprovisioned \
    --node-id 0 \
    --kafka-addr internal://0.0.0.0:19092,external://0.0.0.0:9092 \
    --advertise-kafka-addr internal://redpanda-server:19092,external://localhost:9092 \
    --pandaproxy-addr internal://0.0.0.0:8082,external://0.0.0.0:8083 \
    --advertise-pandaproxy-addr internal://redpanda-server:8082,external://localhost:8083 \
    --schema-registry-addr internal://0.0.0.0:8081,external://0.0.0.0:8084 \
    --rpc-addr redpanda-server:33145 \
    --advertise-rpc-addr redpanda-server:33145

# Wait a moment for Redpanda to start
sleep 2

# Start Redpanda Console
echo "Starting Redpanda Console on http://localhost:8080..."
docker run --rm -d -p 8080:8080 --name redpanda-console \
    --network app-tier \
    -e KAFKA_BROKERS=redpanda-server:19092 \
    docker.redpanda.com/redpandadata/console:latest

echo ""
echo "✓ Redpanda started on localhost:9092"
echo "✓ Redpanda Console UI available at http://localhost:8080"
echo ""
echo "To stop: docker stop redpanda-server redpanda-console"
