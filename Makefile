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
.PHONY: all clean test build

all: build

build: clean $(APPS)

tansivesrv:
	@echo "Building tansivesrv..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansivesrv ./cmd/tansivesrv

tansive-worker:
	@echo "Building tansive-worker..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansive-worker ./cmd/worker

tansive-cli:
	@echo "Building tansive-cli..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansive-cli ./cmd/tansive-cli

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILDDIR)

test:
	@echo "Running tests..."
	$(GOTEST) -p=1 -count=1 ./...
