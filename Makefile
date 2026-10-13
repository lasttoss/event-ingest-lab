.PHONY: test fmt up down logs api gen measure clean

MODE ?= batched
EVENTS ?= 100000

test: ## every gate that does not need a server
	@go vet ./...
	@go test ./... -count=1

fmt:
	@gofmt -l -w .

up: ## start ClickHouse and Vector (the ingest API runs on the host, so it can be restarted mid-experiment)
	@mkdir -p data/wal
	@# Vector bind-mounts this directory. Docker creates a bind-mounted directory it has to create
	@# itself as root, and then the ingest API - which runs on the host, as you - cannot write the
	@# write-ahead log into it. Give it back to the user who is about to write to it.
	@[ -w data/wal ] || docker run --rm -v "$(PWD)/data":/d alpine chown -R "$$(id -u):$$(id -g)" /d
	@docker compose up -d --wait

down:
	@docker compose down -v

logs:
	@docker compose logs -f --tail=50

api: ## run the ingest API on the host: make api MODE=per-event
	@go run ./cmd/api -mode $(MODE) -wal-dir data/wal

gen: ## push synthetic events at the API
	@go run ./cmd/generator -events $(EVENTS)

measure: up ## run every arm and write docs/measurements.md
	@./scripts/measure.sh

clean:
	@rm -rf data/wal/*.ndjson
