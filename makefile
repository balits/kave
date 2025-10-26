APP_NAME = thesis
DOCKERFILE = docker/Dockerfile
COMPOSE3 = docker/docker-compose3.yaml
COMPOSE5 = docker/docker-compose3.yaml

.DEFAULT_GOAL := help

build: ## Build Go binary
	@echo "building ${APP_NAME}..."
	go build -o bin/${APP_NAME} ./pkg/cmd/thesis/main.go

docker-build: ## Build Docker image
	docker buildx build -f docker/Dockerfile -t kvraft .

compose-up3: ## Run 3-node cluster
	docker compose -f docker-compose3.yaml up --build --remove-orphans

compose-up5: ## Run 5-node cluster
	docker compose -f docker-compose5.yaml up --build --remove-orphans

compose-down3:
	docker compose -f docker-compose3.yaml down

compose-down5:
	docker compose -f docker-compose3.yaml down

clean: ## Cleal local builds
	rm -rf bin

help: ## Show commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'