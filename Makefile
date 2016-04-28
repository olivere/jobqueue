all: test

.PHONY: test
test:
	go test ./...

.PHONY: cover
cover:
	go test -coverprofile=cover.coverprofile .
	go tool cover -html=cover.coverprofile
