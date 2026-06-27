.PHONY: up down tidy run-snapshot fmt lint migrate-up migrate-down run-aggregator psql snapshot-balances migrate-force kafka-topics-create run-classification proto run-analytics analytics run-gateway

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

kafka-topics:
	docker exec -it erc20_kafka kafka-topics --bootstrap-server localhost:9092 --list

kafka-consume:
	docker exec -it erc20_kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) --from-beginning


publish-test-job:
	go run scripts/publish_test_job.go

watch-results:
	docker exec -it erc20_kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic snapshot.results --from-beginning

watch-status:
	docker exec -it erc20_kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 \
		--topic snapshot.status --from-beginning

DB_DSN := postgresql://analyzer:analyzer@localhost:5432/erc20?sslmode=disable

migrate-up:
	migrate -path ./migrations -database "$(DB_DSN)" up

migrate-down:
	migrate -path ./migrations -database "$(DB_DSN)" down 1

migrate-force:
	migrate -path ./migrations -database "$(DB_DSN)" force $(V)

run-aggregator:
	go run ./cmd/aggregator

psql:
	docker exect -it erc20_posgres psql -U analyzer -d erc20

# Inspect a finished snapshot (usage: make snapshot-balances ID=<uuid>)
snapshot-balances:
	docker exec -i erc20_postgres psql -U analyzer -d erc20 -c "SELECT state, from_block, block_number, chunks_total, done_seen FROM snapshots WHERE id='$(ID)'; SELECT count(*) AS holders FROM balances WHERE snapshot_id='$(ID)'; SELECT address, balance FROM balances WHERE snapshot_id='$(ID)' ORDER BY balance DESC LIMIT 10;"

kafka-topics-create:
	docker exec -i erc20_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic snapshot.jobs --partitions 6 --replication-factor 1
	docker exec -i erc20_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic snapshot.results --partitions 6 --replication-factor 1
	docker exec -i erc20_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic snapshot.status --partitions 3 --replication-factor 1
	docker exec -i erc20_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic classify.requests --partitions 6 --replication-factor 1
	docker exec -i erc20_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic classify.results --partitions 6 --replication-factor 1


run-classification:
	go run ./cmd/classification-service

proto:
	protoc \
		--go_out=. --go_opt=module=github.com/gfournierPro/erc20-analyzer \
		--go-grpc_out=. --go-grpc_opt=module=github.com/gfournierPro/erc20-analyzer \
		api/proto/analytics.proto

run-analytics:
	go run ./cmd/analytics-service

analytics:
	go run ./cmd/analytics-client $(ID)

run-gateway:
	go run ./cmd/api-gateway