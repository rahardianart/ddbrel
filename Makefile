.PHONY: test integration up down

test:
	go test ./...

up:
	docker compose up -d --wait

down:
	docker compose down -v

integration: up
	DDB_ENDPOINT=http://localhost:4566 go test -tags=integration -count=1 ./...
