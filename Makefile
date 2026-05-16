.PHONY: up down tidy run-snapshot fmt lint

up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

tidy:
	go mod tidy

run-snapshot:
	go run ./cmd/snapshot-service

fmt:
	go fmt ./...

lint:
	go vet ./...
