# Go parameters
GO := go
GOBUILD := $(GO) build
GOCLEAN := $(GO) clean
GOTEST := $(GO) test

# Build directory
BUILDDIR := build

# Applications
APPS := tansivesrv tangent tansive

# Docker configuration
DOCKER_IMAGE_NAME := tansive
DOCKER_TAG := $(shell whoami)-latest

# Targets
.PHONY: all clean test build cli srv worker docker-build docker-build-multiarch docker-build-local docker-test-multiarch

all: build

build: $(APPS)

# Server target
srv:
	@echo "Building tansivesrv..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansivesrv ./cmd/tansivesrv

# Worker target
tangent:
	@echo "Building tangent..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tangent ./cmd/tangent

# CLI target
cli:
	@echo "Building tansive..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansive ./cmd/tansive-cli

# Docker build targets
docker-build:
	@echo "Building Docker images..."
	docker build -t $(DOCKER_IMAGE_NAME)/tansivesrv:$(DOCKER_TAG) -f Dockerfile .
	docker build -t $(DOCKER_IMAGE_NAME)/tangent:$(DOCKER_TAG) -f Dockerfile.tangent .

docker-build-multiarch:
	@echo "Building multi-architecture Docker images..."
	@chmod +x scripts/docker/build-multiarch.sh
	./scripts/docker/build-multiarch.sh $(DOCKER_TAG)

docker-build-local:
	@echo "Building multi-architecture Docker images locally..."
	@chmod +x scripts/docker/build-local.sh
	./scripts/docker/build-local.sh $(DOCKER_TAG)

docker-test-multiarch:
	@echo "Testing multi-architecture Docker images..."
	@chmod +x scripts/docker/test-multiarch.sh
	./scripts/docker/test-multiarch.sh

# Original targets for backward compatibility
tansivesrv: srv

tansive: cli

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILDDIR)

test:
	@echo "Running tests..."
	$(GOTEST) -p=1 -count=1 ./...
