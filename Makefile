.PHONY: up down logs load load-list meta smoke profiles sessions session-new session-run

up:
	docker compose up -d --build

down:
	docker compose --profile load down

logs:
	docker compose logs -f --tail=100 go-api python-api

profiles:
	python ingest/generator/generator.py --list

load:
	docker compose --profile load run --rm generator

load-list:
	docker compose --profile load run --rm generator --list

meta:
	curl -s http://localhost:8081/v1/meta && echo && curl -s http://localhost:8082/v1/meta && echo

sessions:
	python sessions/run.py list

session-new:
	python sessions/run.py new --why "$(WHY)"

session-run:
	python sessions/run.py run $(ID)

smoke:
	curl -s -X POST http://localhost:8081/v1/points -H "Content-Type: application/json" -d "{\"points\":[{\"metric\":\"cpu.usage\",\"value\":42.1,\"labels\":{\"host\":\"dev-001\"}}]}" && echo
	curl -s -X POST http://localhost:8081/v1/latest -H "Content-Type: application/json" -d "{\"metric\":\"cpu.usage\"}" && echo
	curl -s -X POST http://localhost:8082/v1/points -H "Content-Type: application/json" -d "{\"points\":[{\"metric\":\"cpu.usage\",\"value\":42.1,\"labels\":{\"host\":\"dev-001\"}}]}" && echo
	curl -s -X POST http://localhost:8082/v1/latest -H "Content-Type: application/json" -d "{\"metric\":\"cpu.usage\"}" && echo
