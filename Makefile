GO ?= go
BIN := bin/recharge

.PHONY: build test vet lint demo clean

build:
	$(GO) build -o $(BIN) ./cmd/recharge

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

lint: vet
	gofmt -l . | tee /dev/stderr | (! read)

# Render the sample statements. No database, no cluster. Iterate on the pages here.
demo:
	$(GO) run ./cmd/recharge demo       > statement-sample.html
	$(GO) run ./cmd/recharge demo-recon > reconciliation-sample.html
	@echo "wrote statement-sample.html reconciliation-sample.html"

clean:
	rm -rf bin *-sample.html
