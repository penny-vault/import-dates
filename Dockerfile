FROM golang:alpine AS builder
WORKDIR /go/src
RUN apk add git && git clone https://github.com/magefile/mage && cd mage && go run bootstrap.go
COPY ./ .
RUN mage -v build

FROM alpine

# Add timezone data and create pv user
RUN apk add tzdata && adduser -D pv
# Run pv as non-privileged
USER pv
WORKDIR /home/pv

COPY --from=builder /go/src/import-dates /home/pv
ENTRYPOINT ["/home/pv/import-dates"]
