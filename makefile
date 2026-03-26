infra-up: 
	docker compose --env-file .env -f infra/docker-compose.yml up -d
	
infra-down: 
	docker-compose --env-file .env -f infra/docker-compose.yml down