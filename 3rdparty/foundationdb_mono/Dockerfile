FROM --platform=linux/amd64 mono:latest

RUN apt update && \
  apt install -y wget vim build-essential

RUN curl -OL https://go.dev/dl/go1.20.3.linux-amd64.tar.gz && \
  tar -C /usr/local -xzf go1.20.3.linux-amd64.tar.gz

RUN wget https://github.com/apple/foundationdb/releases/download/6.3.22/foundationdb-clients_6.3.22-1_amd64.deb && \
  dpkg -i "foundationdb-clients_6.3.22-1_amd64.deb"

# RUN wget https://github.com/apple/foundationdb/releases/download/7.2.5/foundationdb-clients_7.2.5-1_amd64.deb && \
#   dpkg -i "foundationdb-clients_7.2.5-1_amd64.deb"

#WORKDIR /app

# Set environment variables
#ENV CGO_ENABLED=1
ENV GOPATH=$HOME/go
ENV PATH="/usr/local/go/bin:$GOPATH/bin:${PATH}"

RUN go install github.com/go-delve/delve/cmd/dlv@latest

# Copy the Go source code and build the application
#COPY . .

# The following command will force our app to use the 6.3.22 version of the FDB client.
# You will need the compiled Go application as well as the libfdb_c.so file to run the application.

# When running on the host, you can use the following command to run the application:
# LD_LIBRARY_PATH=. ./fdb_go_client

#RUN go get github.com/apple/foundationdb/bindings/go/src/fdb@6.3.22 && \
#  go build && \
#  cp /usr/lib/libfdb_c.so .

# RUN go get github.com/apple/foundationdb/bindings/go/src/fdb@release-7.2 && \
#   go build && \
#   cp /usr/lib/libfdb_c.so .

# ENTRYPOINT ["tail", "-f", "/dev/null"]

#CMD ["/app/fdb_go_client"]

