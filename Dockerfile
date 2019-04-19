# stage 1: build
FROM golang:1.10-alpine AS builder

# Add source code
RUN mkdir -p /go/src/github.com/searchlight/ruler
ADD . /go/src/github.com/searchlight/ruler

# Build binary
RUN cd /go/src/github.com/searchlight/ruler && \
    GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /go/bin/ruler

# stage 2: lightweight "release"
FROM alpine:latest
LABEL maintainer="nightfury1204"

COPY --from=builder /go/bin/ruler /bin/

ENTRYPOINT [ "/bin/ruler" ]
