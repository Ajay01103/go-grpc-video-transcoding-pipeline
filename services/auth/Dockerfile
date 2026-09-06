FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install dependencies first for caching
COPY go.work go.work.sum* ./
COPY services/auth/go.mod services/auth/go.sum ./services/auth/

# Download module dependencies
RUN cd services/auth && go mod download

# Copy source code
COPY services/auth ./services/auth
COPY proto ./proto

# Build the binary
RUN cd services/auth && CGO_ENABLED=0 go build -o /app/auth-service ./cmd/

# Minimal runtime image
FROM alpine:3.19

WORKDIR /app
COPY --from=builder /app/auth-service .

EXPOSE 50051
CMD ["/app/auth-service"]
