.PHONY: run build test bench stress-producer stress-consumer clean docker-up docker-down

# start the broker
run:
	go run cmd/hermitmq/main.go

# build the binary
build:
	mkdir -p bin
	go build -o bin/hermitmq cmd/hermitmq/main.go

# run unit tests
test:
	go test -v -race ./...

# run disk and network benchmarks
bench:
	go test -bench=. ./internal/broker/...

# load testing utilities
stress-consumer:
	go run cmd/spam_consumer/main.go

stress-producer:
	go run cmd/spam_producer/main.go

# clean up logs and binaries
clean:
	rm -rf bin/
	rm -rf hermitmq-data/

# docker commands
docker-up:
	docker-compose up -d --build

docker-down:
	docker-compose down