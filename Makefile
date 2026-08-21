.PHONY: up down logs load load-list meta smoke profiles sessions session-new session-run session-compare session-preflight

up:
	docker compose up -d --build

down:
	docker compose --profile load down

logs:
	docker compose logs -f --tail=100 go-api csharp-api rust-api

profiles:
	python ingest/generator/generator.py --list

load:
	docker compose --profile load run --rm generator

load-list:
	docker compose --profile load run --rm generator --list

meta:
	curl -s http://localhost:8081/v1/meta && echo && curl -s http://localhost:8082/v1/meta && echo && curl -s http://localhost:8083/v1/meta && echo && curl -s http://localhost:8084/v1/meta && echo

sessions:
	python sessions/run.py list

session-new:
	python sessions/run.py new --why "$(WHY)"

session-run:
	python sessions/run.py run $(ID)

session-compare:
	python sessions/run.py compare $(ID)

session-preflight:
	python sessions/run.py preflight $(ID)

smoke:
	curl -s -X POST http://localhost:8081/v1/write -H "Content-Type: application/json" -d "{\"samples\":[{\"tag_id\":1,\"value\":42.1,\"quality\":192}]}" && echo
	curl -s -X POST http://localhost:8081/v1/locf -H "Content-Type: application/json" -d "{\"tag_ids\":[1],\"at\":\"$$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" && echo
