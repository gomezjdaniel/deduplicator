.PHONY: build
build:
	go build -o bin/deduplicator main.go deduplicator.go

.PHONY: run
run: build
	./bin/deduplicator

.PHONY: test
test:
	go test -v deduplicator.go deduplicator_test.go
