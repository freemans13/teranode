#!/bin/sh

docker exec -it bitcoin-regtest /bitcoin-cli -conf=/bitcoin/bitcoin.conf $@