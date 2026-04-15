KAVE = kave
APP_NAME = $(KAVE)
NAMESPACE = $(APP_NAME)
CHART = ./charts/kave
KUBECONFIG ?= .kube/config

DOCKERFILE = docker/Dockerfile
COMPOSE1 = docker-compose1.yaml
COMPOSE3 = docker-compose3.yaml
COMPOSE5 = docker-compose5.yaml
IMG_NAME = ghcr.io/balits/kave

.DEFAULT_GOAL := help

build-go: ## Build Go binary
	@echo ">> building ${APP_NAME}..."
	go build -o bin/${APP_NAME} cmd/kave/main.go

build-img: ## Build Docker image
	@echo ">> builing img: ${IMG_NAME}..."
	$(MAKE) build
	docker build -t ${IMG_NAME}:latest .

build-img-push:
	$(MAKE) build-img
	@echo ">> pushing img to ghcr: ${IMG_NAME}..."
	docker push ${IMG_NAME}:latest


up1build: ## Builds image and runs 1-node cluster
	$(MAKE) build-img
	$(MAKE) up1

up1: ## Run 1-node cluster
	docker compose -f $(COMPOSE1) -p $(APP_NAME) up --remove-orphans

up3: ## Run 3-node cluster
	docker compose -f $(COMPOSE3) -p $(APP_NAME) up --remove-orphans

down3:
	docker compose -f $(COMPOSE3) -p $(APP_NAME) down --volumes

up3build: ## Builds image and runs 3-node cluster
	$(MAKE) build-img
	$(MAKE) up3

test-unit:
	go test -v ./internal/... --timeout=90s

test-integ:
	go test ./test/integration/... --timeout 5m -p 1

test-smoke:
	go test -v ./test/smoke/...

test-race:
	go test -v -race -count=3 ./internal/... 

kind-cluster-create: ## Creates a k8s cluster with kind
	kind create cluster --name kave

kind-verify: ## Verifyies kind cluster
	kubectl get nodes

helm-validate: ## validate template
	helm template $(APP_NAME) ./charts/kave

helm-lint: ## Lint for mistakes
	helm lint ./charts/kave

helm-uninstall:
	@echo ">> uninstalling chart: ${APP_NAME}..."
	helm uninstall kave -n kave --ignore-not-found --kubeconfig .kube/config

helm-upgrade-install:
	@echo ">> upgrading/installing chart: ${APP_NAME}..."
	helm upgrade $(APP_NAME) $(CHART) --install --namespace $(NAMESPACE)
		--create-namespace \
		--wait \
		--timeout 3m

local-helm-recreate:
	$(MAKE) helm-uninstall
	$(MAKE) helm-upgrade-install

gh-ci:
	gh workflow run ci.yaml --ref feature/ci

local-kube-recreate-cluster:
	@echo ">> deleting namesapce ${NAMESPACE}..."
	kubectl delete namespace kave --kubeconfig .kube/config
	@echo ">> creating namesapce ${NAMESPACE}..."
	kubectl create namespace kave --kubeconfig .kube/config

local-kube-restart-rollout:
	@echo ">> rollout/restart kave-voter statefulset..."
	kubectl rollout restart statefulset/kave-voter -n $(NAMESPACE) --kubeconfig .kube/config

wipe-recreate-cluster: ## Wipe and redeploy helm chart
	@echo ">> 1. step: uninstall any chart"
	$(MAKE) helm-uninstall
	@echo ">> 1. step: ok"

	@echo ">> 2. step: deleting k8s persistentVolumeClaims..."
	kubectl delete pvc --all -n $(NAMESPACE) --ignore-not-found --kubeconfig $(KUBECONFIG)
	kubectl wait --for=delete pvc --all -n kave --timeout=1m || true
	@echo ">> 2. step: ok"

	@echo ">> 3. step: insalling/upgrading chart ${APP_NAME}..."
	$(MAKE) helm-upgrade-install
	@echo ">> 3. step: ok"
	@echo ">> DONE"

wait-cluster-ready:
	@echo ">> 1. waiting on rollout status"
	kubectl rollout status statefulset/kave-voter -n $(NAMESPACE) --timeout=3m

	@echo ">> 2. printing final pod state"
	kubectl get pods -n $(NAMESPACE)

k9s:
	./bin/k9s --kubeconfig .kube/config 
fmt:
	./bin/golangci-lint fmt
lint:
	./bin/golangci-lint run

.PHONY: build build-img up3 down3 up3build test test-race kluster-create, kluster-verify helm-validate helm-lint helm-uninstall gh-ci
