.SILENT:

.PHONY: lint
lint:
	@go tool golangci-lint run ./...

.PHONY: lint-fix
lint-fix:
	go tool golangci-lint run ./... --fix --timeout 650s