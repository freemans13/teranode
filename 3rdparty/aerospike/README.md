
# Aerospike in dockerr

```shell
docker run -d --name aerospike -p 3800-3802:3000-3002 aerospike:ce-6.3.0.1_1
```

## for permanent storage

```shell
docker run -d --name aerospike -p 3800-3802:3000-3002 -v /opt/aerospike/data:/opt/aerospike/data -v /opt/aerospike/etc:/opt/aerospike/etc aerospike:ce-6.3.0.1_1
```
