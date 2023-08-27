FROM golang:1.21-alpine3.18 AS builder
WORKDIR /

# Disable cgo to create a static binary.
ENV CGO_ENABLED="0"

# Compile for 64-bit Linux
ENV GOOS="linux"
ENV GOARCH="amd64"

# Cache dependencies
ADD processor.go go.mod go.sum ./
RUN go mod download

# Build
RUN go build -a -o app processor.go

# Copy artifacts to a clean image
FROM scratch
WORKDIR /
EXPOSE 5000
COPY --from=builder /app /
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt


CMD ["/app"]