FROM golang:1.13 AS builder

COPY *.go go.mod go.sum src/
COPY blacklists src/blacklists
COPY cmd src/cmd
COPY external src/external
COPY reporter src/reporter
RUN cd src && GOBIN=$(realpath ..) GO111MODULE=on go install github.com/src-d/identity-matching/cmd/match-identities

FROM ubuntu:18.04

COPY --from=builder /go/match-identities /usr/local/bin

COPY parquet2sql/requirements.txt .
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-suggests --no-install-recommends ca-certificates libsnappy1v5 libsnappy-dev python3 python3-distutils python3-dev gcc g++ wget && \
    wget -O - https://bootstrap.pypa.io/get-pip.py | python3 && \
    pip3 install -r requirements.txt && \
    rm requirements.txt && \
    apt-get remove -y libsnappy-dev wget python3-dev gcc g++ && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY parquet2sql/parquet2sql.py /usr/local/bin
COPY identities2sql.sh /usr/local/bin

CMD ["identities2sql.sh"]