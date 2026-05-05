FROM golang:1.23-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o hyperwallettracker ./cmd/

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/hyperwallettracker .
COPY --from=builder /app/web/static ./web/static

RUN mkdir -p /data
ENV DB_PATH=/data/tracker.db
ENV PORT=8080

EXPOSE 8080
CMD ["./hyperwallettracker"]
