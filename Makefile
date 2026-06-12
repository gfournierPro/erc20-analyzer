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