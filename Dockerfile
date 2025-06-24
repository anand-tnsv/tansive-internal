# Multi-stage build for tansivesrv
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Generate go.sum and tidy dependencies
RUN go mod tidy

# Build the tansivesrv binary
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o tansivesrv ./cmd/tansivesrv

# Final stage
FROM alpine:3.19

# Install runtime dependencies and update packages
RUN apk update && apk upgrade && \
    apk add --no-cache ca-certificates tzdata && \
    rm -rf /var/cache/apk/*

# Create non-root user
RUN addgroup -g 1001 -S tansive && \
    adduser -u 1001 -S tansive -G tansive

# Create necessary directories
RUN mkdir -p /etc/tansive /var/log/tansive && \
    chown -R tansive:tansive /etc/tansive /var/log/tansive

# Copy binary from builder stage
COPY --from=builder /app/tansivesrv /usr/local/bin/tansivesrv

# Copy configuration file
COPY tansivesrv.conf /etc/tansive/tansivesrv.conf

# Set ownership of the binary
RUN chown tansive:tansive /usr/local/bin/tansivesrv && \
    chmod +x /usr/local/bin/tansivesrv

# Switch to non-root user
USER tansive

# Expose default port
EXPOSE 8678

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8678/health || exit 1

# Set the entrypoint
ENTRYPOINT ["/usr/local/bin/tansivesrv"]

# Default command (can be overridden)
CMD ["--config", "/etc/tansive/tansivesrv.conf"] 