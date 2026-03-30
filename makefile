infra-up: 
	docker compose --env-file .env -f infra/docker-compose.yml up -d
	
infra-down: 
	docker-compose --env-file .env -f infra/docker-compose.yml down

infra-rebuild:
	docker compose --env-file .env -f infra/docker-compose.yml build

producer-up:
	docker compose --env-file .env -f producer/docker-compose.yml up

producer-down:
	docker compose --env-file .env -f producer/docker-compose.yml down
	docker run --rm -v producer_spark-data-volume:/data alpine rm -rf /data/users

producer-rebuild:
	docker compose --env-file .env -f producer/docker-compose.yml build

up:
	make infra-up
	make producer-up

down:
	make infra-down
	make producer-down