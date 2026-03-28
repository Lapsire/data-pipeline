infra-up: 
	docker compose --env-file .env -f infra/docker-compose.yml up -d
	
infra-down: 
	docker-compose --env-file .env -f infra/docker-compose.yml down

producer-up:
	docker compose --env-file .env -f producer/docker-compose.yml up

producer-down:
	docker compose --env-file .env -f producer/docker-compose.yml down
	docker run --rm -v producer_spark-data-volume:/data alpine rm -rf /data/users

up:
	make infra-up
	make producer-up

down:
	make infra-down
	make producer-down