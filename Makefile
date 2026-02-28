.PHONY: help build-ledger build-all push-ledger push-all docker-up docker-down clean

# Variáveis
DOCKER_REGISTRY ?= fidelissauro
LEDGER_IMAGE = $(DOCKER_REGISTRY)/ledger-event-sourcing
BALANCE_IMAGE = $(DOCKER_REGISTRY)/ledger-balance
BALANCE_API_IMAGE = $(DOCKER_REGISTRY)/ledger-balance-api
STATEMENT_IMAGE = $(DOCKER_REGISTRY)/ledger-statement
STATEMENT_API_IMAGE = $(DOCKER_REGISTRY)/ledger-statement-api
SIMULADOR_IMAGE = $(DOCKER_REGISTRY)/ledger-simulador
TAG ?= latest
PLATFORMS = linux/amd64,linux/arm64

help: ## Exibe esta mensagem de ajuda
	@echo "Comandos disponíveis:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

build-ledger: ## Build do ledger para múltiplas plataformas
	@echo "Building ledger image..."
	cd ledger && docker buildx build -t $(LEDGER_IMAGE):$(TAG) --platform $(PLATFORMS) .

build-balance: ## Build do balance para múltiplas plataformas
	@echo "Building balance image..."
	cd balance && docker buildx build -t $(BALANCE_IMAGE):$(TAG) --platform $(PLATFORMS) .

build-balance-api: ## Build do balance-api para múltiplas plataformas
	@echo "Building balance-api image..."
	cd balance-api && docker buildx build -t $(BALANCE_API_IMAGE):$(TAG) --platform $(PLATFORMS) .

build-statement: ## Build do statement para múltiplas plataformas
	@echo "Building statement image..."
	cd statement && docker buildx build -t $(STATEMENT_IMAGE):$(TAG) --platform $(PLATFORMS) .

build-statement-api: ## Build do statement-api para múltiplas plataformas
	@echo "Building statement-api image..."
	cd statement-api && docker buildx build -t $(STATEMENT_API_IMAGE):$(TAG) --platform $(PLATFORMS) .

build-simulador: ## Build do simulador para múltiplas plataformas
	@echo "Building simulador image..."
	cd simulador && docker buildx build -t $(SIMULADOR_IMAGE):$(TAG) --platform $(PLATFORMS) .

build-all: build-ledger build-balance build-balance-api build-statement build-statement-api build-simulador ## Build de todas as imagens

push-ledger: build-ledger ## Build e push do ledger
	@echo "Pushing ledger image..."
	cd ledger && docker buildx build -t $(LEDGER_IMAGE):$(TAG) --platform $(PLATFORMS) --push .

push-balance: build-balance ## Build e push do balance
	@echo "Pushing balance image..."
	cd balance && docker buildx build -t $(BALANCE_IMAGE):$(TAG) --platform $(PLATFORMS) --push .

push-balance-api: build-balance-api ## Build e push do balance-api
	@echo "Pushing balance-api image..."
	cd balance-api && docker buildx build -t $(BALANCE_API_IMAGE):$(TAG) --platform $(PLATFORMS) --push .

push-statement: build-statement ## Build e push do statement
	@echo "Pushing statement image..."
	cd statement && docker buildx build -t $(STATEMENT_IMAGE):$(TAG) --platform $(PLATFORMS) --push .

push-statement-api: build-statement-api ## Build e push do statement-api
	@echo "Pushing statement-api image..."
	cd statement-api && docker buildx build -t $(STATEMENT_API_IMAGE):$(TAG) --platform $(PLATFORMS) --push .

push-simulador: build-simulador ## Build e push do simulador
	@echo "Pushing simulador image..."
	cd simulador && docker buildx build -t $(SIMULADOR_IMAGE):$(TAG) --platform $(PLATFORMS) --push .

push-all: push-ledger push-balance push-balance-api push-statement push-statement-api push-simulador ## Build e push de todas as imagens

docker-up: ## Sobe todos os serviços com docker-compose
	docker-compose up -d

docker-down: ## Para todos os serviços
	docker-compose down

docker-logs: ## Exibe logs de todos os serviços
	docker-compose logs -f

docker-restart: docker-down docker-up ## Reinicia todos os serviços

clean: ## Remove containers, volumes e imagens locais
	docker-compose down -v
	docker system prune -f

setup-buildx: ## Configura Docker Buildx para multi-plataforma
	docker buildx create --name multiarch --driver docker-container --use
	docker buildx inspect --bootstrap

test-ledger: ## Executa testes do ledger
	cd ledger && go test -v ./...

test-balance: ## Executa testes do balance
	cd balance && go test -v ./...

test-statement: ## Executa testes do statement
	cd statement && go test -v ./...

test-simulador: ## Executa testes do simulador
	cd simulador && go test -v ./...

test-all: test-ledger test-balance test-statement test-simulador ## Executa todos os testes

format: ## Formata código Go de todos os serviços
	cd ledger && go fmt ./...
	cd balance && go fmt ./...
	cd balance-api && go fmt ./...
	cd statement && go fmt ./...
	cd statement-api && go fmt ./...
	cd simulador && go fmt ./...
