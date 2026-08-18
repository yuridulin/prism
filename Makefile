.PHONY: up down logs load meta smoke

up:
	docker compose up -d --build

down:
	docker compose --profile load down

logs:
	docker compose logs -f --tail=100 go-api python-api

load:
	docker compose --profile load up generator

meta:
	curl -s http://localhost:8081/v1/meta && echo && curl -s http://localhost:8082/v1/meta && echo

smoke:
	curl -s -X POST http://localhost:8081/v1/points -H "Content-Type: application/json" -d "{\"points\":[{\"metric\":\"cpu.usage\",\"value\":42.1,\"labels\":{\"host\":\"dev-001\"}}]}"
	curl -s "http://localhost:8081/v1/latest?metric=cpu.usage" && echo
	curl -s -X POST http://localhost:8082/v1/points -H "Content-Type: application/json" -d "{\"points\":[{\"metric\":\"cpu.usage\",\"value\":42.1,\"labels\":{\"host\":\"dev-001\"}}]}"
	curl -s "http://localhost:8082/v1/latest?metric=cpu.usage" && echo
