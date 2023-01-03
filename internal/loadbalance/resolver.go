package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/ac0mz/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// Resolver はgRPCの resolver.Builder インタフェースと resolver.Resolver インタフェースを実装する。
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn // ユーザのクライアントコネクション (gRPCがリゾルバにコネクションを渡し、リゾルバが発見したサーバで更新する)
	resolverConn  *grpc.ClientConn    // リゾルバ自身のコネクション (GetServers APIを呼び出す)
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

// Build はサーバを発見できるリゾルバ構築に必要なデータと、リゾルバが発見したサーバで更新するクライアントコネクション
// を受け取り、リゾルバが GetServers API を呼び出せるように、サーバへのクライアントコネクションを設定する。
func (r *Resolver) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{ "loadBalancingConfig": [{ "%s": {} }] }`, Name),
	)
	var err error
	// WARNING:
	//  target.Endpoint が deprecated だが、推奨の target.URL.Path を使用すると agent_test.go のテスト実行が永遠に完了しなくなる
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "proglog"

// Scheme はリゾルバのスキーム識別子を返却する。
//
//	NOTE:
//	 gRPC.Dial を呼び出すと、gRPCは与えられたターゲットアドレスからスキームを解析し、一致するリゾルバ (※) を探す。
//	 ※デフォルトはDNSリゾルバ
//	 当該システムの場合、ターゲットアドレスの形式は proglog://own-service-address のようになる。
func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

var _ resolver.Resolver = (*Resolver)(nil)

// ResolveNow はターゲットを解決し、サーバを発見し、ロードバランサが選択できるサーバを知らせるために
// サーバとのクライアントコネクションを更新する。
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	// gRPCは当該メソッドを並列に呼び出す可能性があるため、ロックしてゴルーチン間のアクセスを保護
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)
	// クラスタを取得してClientConnの状態を更新する
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr:       server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader), // ロードバランサ用の様々なデータを含むマップ。どのサーバがリーダーorフォロワーかをピッカーに伝える
		})
	}

	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close はBuildで作成したサーバへのコネクション (リゾルバ) を閉じる。
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}
}
