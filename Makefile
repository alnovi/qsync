.SILENT:

.PHONY: lint
lint:
	@go tool golangci-lint run ./...

.PHONY: lint-fix
lint-fix:
	go tool golangci-lint run ./... --fix --timeout 650s

.PHONY: test
test:
	@go tool gotestsum --format=testname -- -count=1 -coverpkg=github.com/alnovi/qsync/v2,github.com/alnovi/qsync/v2/utils -coverprofile=./coverage.out ./...
	@go tool cover -html=./coverage.out
	@rm ./coverage.out