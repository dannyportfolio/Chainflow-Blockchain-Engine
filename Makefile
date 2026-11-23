.PHONY: fmt vet test test-race build clean

GO ?= go

fmt:
	$(GO) fmt ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test -v ./...

test-race:
	$(GO) test -race -count=1 ./...

check: fmt vet test-race

build:
	$(GO) build ./...

clean:
	rm -rf bin/ coverage.out coverage.html
