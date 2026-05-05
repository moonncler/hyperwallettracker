FROM golang:1.25-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o hyperwallettracker ./cmd/

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/hyperwallettracker .

RUN mkdir -p /data

ENV DB_PATH=/data/tracker.db

CMD ["./hyperwallettracker"]
