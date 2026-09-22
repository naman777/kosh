.PHONY: up down test test-full vet sim sim-soak

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

# Replay one simulation seed with its trace: make sim SEED=42
sim:
	go run ./cmd/sim -seed $(SEED) -v

# The local soak: 10,000 seeds.
sim-soak:
	go run ./cmd/sim -seeds 10000 -parallel 8
