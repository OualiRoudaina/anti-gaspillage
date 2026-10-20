.PHONY: up down logs ps load bench chaos topics

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

topics:
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list

# A implementer dans les prochaines taches
load:
	@echo "TODO : export des lots + generateur de ventes"

bench:
	@echo "TODO : benchmarks (etape 4)"

chaos:
	@echo "TODO : tests de panne (etape 5)"
