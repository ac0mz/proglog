.PHONY: init gencert compile test test-clean help
CONFIG_PATH=${HOME}/.proglog/

init: ## 生成された証明書の格納場所を作成する
	mkdir -p ${CONFIG_PATH}

gencert: ## cfsslによりCAとサーバの証明書および秘密鍵を生成する
	cfssl gencert -initca test/ca-csr.json | cfssljson -bare ca
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=server \
		test/server-csr.json | cfssljson -bare server
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="root" \
		test/client-csr.json | cfssljson -bare root-client
	cfssl gencert \
		-ca=ca.pem \
		-ca-key=ca-key.pem \
		-config=test/ca-config.json \
		-profile=client \
		-cn="nobody" \
		test/client-csr.json | cfssljson -bare nobody-client
	mv *.pem *.csr ${CONFIG_PATH}

compile: ## protobufをコンパイルする
	protoc api/v1/*.proto \
      --go_out=. \
      --go-grpc_out=. \
      --go_opt=paths=source_relative \
      --go-grpc_opt=paths=source_relative \
      --proto_path=.

$(CONFIG_PATH)/model.conf:
	cp test/model.conf $(CONFIG_PATH)/model.conf
$(CONFIG_PATH)/policy.csv:
	cp test/policy.csv $(CONFIG_PATH)/policy.csv

test: $(CONFIG_PATH)/model.conf $(CONFIG_PATH)/policy.csv  ## ACL設定ファイルを読み込み、テストを実行する
	go test -v -race -cover ./...
test-clean: ## テスト結果のキャッシュを初期化して、テストを実行する
	go clean -testcache && make test

help: ## makeコマンドのヘルプ
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS=":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

TAG ?= 0.0.1

build-docker: ## Dockerビルド
	docker build -t github.com/ac0mz/proglog:$(TAG) .

kind-start: ## Dockerコンテナをノードとしてローカルk8sクラスタを作成＆実行
	kind create cluster
kind-load-img: ## ビルドしたDockerイメージをKindクラスタにロード
	kind load docker-image github.com/ac0mz/proglog:$(TAG)

helm-show-template: ## helmチャートの確認
	helm template proglog deploy/proglog
helm-install: ## helmチャートをクラスタにインストール
	helm install proglog deploy/proglog
helm-uninstall: ## helmチャートをクラスタからアンインストール
	helm uninstall proglog deploy/proglog

port-forward-local: ## ローカル環境上のk8s(ポッドやサービス)に対してホストのポートに転送
	kubectl relay host/proglog-0.proglog.default.svc.cluster.local 8400

pod-list: ## 現在のクラスタ内のポッド一覧を表示
	kubectl get pods
