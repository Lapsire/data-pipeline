infra-up: 
	docker compose --env-file .env -f infra/docker-compose.yml up -d
	
infra-down: 
	docker-compose --env-file .env -f infra/docker-compose.yml down

producer-up:
	docker compose --env-file .env -f producer/docker-compose.yml up

producer-down:
	docker compose --env-file .env -f producer/docker-compose.yml down

up:
	make infra-up
	make producer-up

down:
	make infra-down
	make producer-down