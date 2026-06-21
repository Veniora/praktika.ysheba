FROM golang:1.22-alpine AS builder

WORKDIR /app

# Copy dependency definition
COPY go.mod ./

# Copy source code
COPY pkg/ ./pkg/
COPY cmd/ ./cmd/

# Build binaries
RUN go build -o manager cmd/manager/main.go
RUN go build -o broker cmd/broker/main.go
RUN go build -o demo cmd/demo/main.go

# Production stage
FROM alpine:latest

WORKDIR /app

# Install bash and curl for demo and testing
RUN apk add --no-cache bash curl

COPY --from=builder /app/manager /app/manager
COPY --from=builder /app/broker /app/broker
COPY --from=builder /app/demo /app/demo

EXPOSE 8080 8081 8082
