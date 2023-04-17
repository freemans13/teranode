FROM ubuntu:22.04

RUN apt-get update
RUN apt-get -y install curl
RUN curl -OL https://download.bitcoinsv.io/bitcoinsv/1.0.14/bitcoin-sv-1.0.14-x86_64-linux-gnu.tar.gz
RUN tar zxvf bitcoin-sv-1.0.14-x86_64-linux-gnu.tar.gz


FROM ubuntu:22.04

RUN mkdir /bitcoin

COPY bitcoin.conf /bitcoin/bitcoin.conf
COPY --from=0 /bitcoin-sv-1.0.14 /bitcoin-sv-1.0.14

RUN ln -s /bitcoin-sv-1.0.14/bin/bitcoin-cli /bitcoin-cli
# RUN apt-get -y install libc6

# rpc
EXPOSE 18332/tcp
# p2p
EXPOSE 18333/tcp
# zmq
EXPOSE 28332/tcp

ENTRYPOINT ["/bitcoin-sv-1.0.14/bin/bitcoind", "-datadir=/bitcoin", "-conf=/bitcoin/bitcoin.conf", "-printtoconsole", "-standalone"]
