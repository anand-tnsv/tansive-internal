# Go parameters
GO := go
GOBUILD := $(GO) build
GOCLEAN := $(GO) clean
GOTEST := $(GO) test

# Protoc parameters
PROTOC := protoc
PROTO_DIR := internal/tangent/session/proto
PROTO_FILES := $(PROTO_DIR)/skill.proto
GOPATH := $(shell go env GOPATH)
PATH := $(GOPATH)/bin:$(PATH)

# Build directory
BUILDDIR := build

# Applications
APPS := tansivesrv tangent tansive-cli

# Targets
.PHONY: all clean test build cli srv worker proto

all: proto build

build: $(APPS)

# Proto compilation
proto:
	@echo "Compiling proto files..."
	PATH="$(GOPATH)/bin:$$PATH" $(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

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
	@echo "Building tansive-cli..."
	@mkdir -p $(BUILDDIR)
	$(GOBUILD) -o $(BUILDDIR)/tansive-cli ./cmd/tansive-cli

# Original targets for backward compatibility
tansivesrv: srv

tansive-cli: cli

clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -rf $(BUILDDIR)

test:
	@echo "Running tests..."
	$(GOTEST) -p=1 -count=1 ./...
