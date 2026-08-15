FROM golang:1.22-alpine AS builder

WORKDIR /build

COPY mirror_worker.go .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o worker mirror_worker.go

FROM alpine:latest

WORKDIR /app

COPY --from=builder /build/worker .

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:3000/health || exit 1

CMD ["./worker"]