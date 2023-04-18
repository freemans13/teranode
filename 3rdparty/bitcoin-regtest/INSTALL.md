# Bitcoin Regtest

This is a simple docker setup for running a Bitcoin regtest node.  Please note that using the --platform flag is required to run this image on an M1 Mac.  This is because the official bitcoin binaries have been compiled for amd64 and not arm64.  This flag is benign on other amd64 machines.

## Usage

```bash
# Build the image
docker build --platform=linux/amd64 -t eu.gcr.io/ubsv-383015/bitcoin-regtest:1.0.14 .

docker push eu.gcr.io/ubsv-383015/bitcoin-regtest:1.0.14


# Create a volume
docker volume create bitcoin-regtest

# Start the container
docker run --platform=linux/amd64 -d --rm \
  --name bitcoin-regtest \
  -p 18332:18332 -p 18333:18333 -p 28332:28332 \
  -v bitcoin-regtest:/bitcoin \
  eu.gcr.io/ubsv-383015/bitcoin-regtest:1.0.14


# Stop the container
docker stop bitcoin-regtest

# Start the container again
docker start bitcoin-regtest

# Remove the container
docker rm bitcoin-regtest
```


To run bitcoin-cli, run the following command:

```bash
docker exec -it bitcoin-regtest /bitcoin-cli <command>
  ```

