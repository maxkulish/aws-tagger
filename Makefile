# Binary name
BINARY_NAME=aws-tagger

# Go related variables
GOBASE=$(shell pwd)
GOBIN=$(GOBASE)/bin
GOFILES=$(wildcard *.go)

# Use the Go compiler from environment or default
GO?=go

# Build-time variables
VERSION?=1.0.0
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
COMMIT_HASH=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Build flags
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.CommitHash=$(COMMIT_HASH)"

# Make is verbose in Linux. Make it silent.
MAKEFLAGS += --silent

.PHONY: all build clean test coverage vet lint fmt help

## Default: run build
all: build

## Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	mkdir -p $(GOBIN)
	$(GO) build $(LDFLAGS) -o $(GOBIN)/$(BINARY_NAME) .
	@echo "Done building."
	@echo "Run \"$(GOBIN)/$(BINARY_NAME)\" to launch the application."

## Run the application
run: build
	$(GOBIN)/$(BINARY_NAME)

## Clean build files
clean:
	@echo "Cleaning build cache..."
	@$(GO) clean
	@rm -rf $(GOBIN)
	@echo "Done cleaning."

## Run tests
test:
	@echo "Running tests..."
	$(GO) test -v ./...

## Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html

## Run go vet
vet:
	@echo "Running go vet..."
	$(GO) vet ./...

## Run go mod tidy
tidy:
	@echo "Running go mod tidy..."
	$(GO) mod tidy

## Run golint
lint:
	@echo "Running golint..."
	golint ./...

## Format code
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

## Install dependencies
deps:
	@echo "Installing dependencies..."
	$(GO) mod download
	$(GO) mod tidy

rebuild: clean build

## Build for multiple platforms
build-all: clean
	@echo "Building for multiple platforms..."
	# Linux
	GOOS=linux GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(GOBIN)/$(BINARY_NAME)-linux-amd64 .
	# MacOS
	GOOS=darwin GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(GOBIN)/$(BINARY_NAME)-darwin-amd64 .
	# Windows
	GOOS=windows GOARCH=amd64 $(GO) build $(LDFLAGS) -o $(GOBIN)/$(BINARY_NAME)-windows-amd64.exe .
	@echo "Done building for all platforms."

## Show help
help:
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
