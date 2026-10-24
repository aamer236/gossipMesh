FROM golang:alpine AS builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/node .

FROM alpine:latest
RUN apk --no-cache add iproute2 iptables curl ca-certificates
WORKDIR /app
COPY --from=builder /app/node .

EXPOSE 8000
CMD ["./node"]
