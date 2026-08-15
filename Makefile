.PHONY: up down test test-full vet

up:
	docker compose up -d --wait

down:
	docker compose down

vet:
	go vet ./...

test:
	go test -race -count=1 ./...

# Runs the 1M-transfer DB property test, which takes several minutes.
test-full:
	go test -race -count=1 ./internal/ledger -run TestPropertyDB -transfers=1000000 -timeout 60m
