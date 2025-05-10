# Go parameters
GO := go
GOBUILD := $(GO) build
GOCLEAN := $(GO) clean
GOTEST := $(GO) test

# Build directory
BUILDDIR := build

# Applications
APPS := tansivesrv tansive-worker tansive-cli

# Targets
.PHONY: all clean test build cli srv worker

all: build

build: clean $(APPS)

# Server target
srv: clean
	@echo "Building tansivesrv..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansivesrv ./cmd/tansivesrv

# Worker target
worker: clean
	@echo "Building tansive-worker..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansive-worker ./cmd/worker

# CLI target
cli: clean
	@echo "Building tansive-cli..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansive-cli ./cmd/tansive-cli

# Original targets for backward compatibility
tansivesrv: srv

tansive-worker: worker

tansive-cli: cli

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILDDIR)

test:
	@echo "Running tests..."
	$(GOTEST) -p=1 -count=1 ./...
