FROM golang:1.12 AS builder

COPY . src/identity-matching
RUN cd src/identity-matching && GO111MODULE=on make build

FROM python:3.7

WORKDIR /home/identity-matching
RUN apt-get update && apt-get install -y libsnappy-dev
COPY --from=builder /go/src/identity-matching/build/bin/match-identities \
        /usr/local/bin/match-identities
COPY . identity-matching/
RUN cd identity-matching/parquet2sql && pip install -r requirements.txt

COPY identities2sql.sh identities2sql.sh

RUN chmod +x identities2sql.sh
CMD ["./identities2sql.sh"]