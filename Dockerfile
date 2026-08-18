# stage 1 build
FROM golang:1.25-alpine AS builder

WORKDIR /app

# copy go mod and go sum files if dependencies appear
COPY go.* ./
RUN go mod download

# copy the source code
COPY . .

# build the binary using static linking without cgo for maximum speed
RUN CGO_ENABLED=0 GOOS=linux go build -o hermitmq cmd/hermitmq/main.go

# stage 2 run
FROM alpine:latest

WORKDIR /app

# copy the compiled binary and config
COPY --from=builder /app/hermitmq .
COPY hermitmq.json .

# create a directory for wal logs
RUN mkdir hermitmq-data

EXPOSE 9092

CMD ["./hermitmq"]