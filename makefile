APP_NAME = thesis
DOCKERFILE = docker/Dockerfile
COMPOSE3 = docker-compose3.yaml
COMPOSE5 = docker-compose5.yaml

.DEFAULT_GOAL := help

build: ## Build Go binary
	@echo "building ${APP_NAME}..."
	go build -o bin/${APP_NAME} internal/cmd/thesis/main.go

docker-build: ## Build Docker image
	docker build -t thesis_node_image .

up3build: docker-build up3

up3: ## Run 3-node cluster
	docker compose -f $(COMPOSE3) -p $(APP_NAME) up --remove-orphans


up3: ## Run 3-node cluster
	docker compose -f $(COMPOSE3) -p $(APP_NAME) up --remove-orphans

down3:
	docker compose -f $(COMPOSE3) -p $(APP_NAME) down --volumes

# up5: ## Run 5-node cluster
# 	docker compose -f $(COMPOSE5) -p $(APP_NAME) up  --remove-orphans
# down5: 
# 	docker compose -f $(COMPOSE5) -p $(APP_NAME) down --volumes

clean: ## Cleal local builds
	rm -rf bin

help: ## Show commands
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'