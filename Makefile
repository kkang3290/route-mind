GO ?= go
BASE_REF ?= HEAD
NAME ?=
STEPS ?= 1
GOVULNCHECK_VERSION := v1.1.4
.PHONY: test race integration vet build check mock run migration-new migration-checksum migration-verify migrate-status migrate-up migrate-down vuln

test:
	$(GO) test ./...
race:
	$(GO) test -race ./...
integration:
	@test -n "$$TEST_DATABASE_URL" || { printf '%s\n' 'TEST_DATABASE_URL must select a disposable *_test database'; exit 1; }
	$(GO) test -race -tags=integration ./...
vet:
	$(GO) vet ./...
build:
	mkdir -p bin
	$(GO) build -trimpath -o bin/route-mind ./cmd/route-mind
	$(GO) build -trimpath -o bin/migrate ./cmd/migrate
	$(GO) build -trimpath -o bin/mock-provider ./cmd/mock-provider
check: test race vet migration-verify build
mock:
	$(GO) run ./cmd/mock-provider
run:
	$(GO) run ./cmd/route-mind -conf configs/config.yaml
migration-new:
	$(GO) run ./cmd/migrate new --dir=migrations --name=$(NAME)
migration-checksum:
	$(GO) run ./cmd/migrate checksum --dir=migrations
migration-verify:
	$(GO) run ./cmd/migrate verify --dir=migrations --base-ref=$(BASE_REF)
migrate-status:
	$(GO) run ./cmd/migrate status
migrate-up:
	$(GO) run ./cmd/migrate up
migrate-down:
	$(GO) run ./cmd/migrate down --steps=$(STEPS) --ack-data-loss
vuln:
	$(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) ./...
