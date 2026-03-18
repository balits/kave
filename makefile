APP_NAME = kave
DOCKERFILE = docker/Dockerfile
COMPOSE3 = docker-compose3.yaml
COMPOSE5 = docker-compose5.yaml

.DEFAULT_GOAL := help

build-go: ## Build Go binary
	@echo "building ${APP_NAME}..."
	go build -o bin/${APP_NAME} cmd/kave/main.go

build-img: ## Build Docker image
	$(MAKE) build
	docker build -t kave .

up3: ## Run 3-node cluster
	docker compose -f $(COMPOSE3) -p $(APP_NAME) up --remove-orphans

down3:
	docker compose -f $(COMPOSE3) -p $(APP_NAME) down --volumes

up3build: ## Builds image and runs 3-node cluster
	$(MAKE) build-img
	$(MAKE) up3

test:
	go test -v ./internal/...

.PHONY: build build-img up3 down3 up3build