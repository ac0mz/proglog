compile: ## protobufをコンパイルする
	protoc api/v1/*.proto \
      --go_out=. \
      --go-grpc_out=. \
      --go_opt=paths=source_relative \
      --go-grpc_opt=paths=source_relative \
      --proto_path=.

test: ## テストを実行する
	go test -v -race ./...

test-clean: ## テスト結果のキャッシュを初期化して、テストを実行する
	go clean -testcache && make test

help: ## makeコマンドのヘルプ
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS=":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
