package loadbalance

import (
	"net"
	"net/url"
	"testing"

	api "github.com/ac0mz/proglog/api/v1"
	"github.com/ac0mz/proglog/internal/config"
	"github.com/ac0mz/proglog/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

func TestResolver(t *testing.T) {
	// テスト用リゾルバがいくつかのサーバを発見するためにサーバを設定
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)
	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &mockGetServers{},
	}, grpc.Creds(serverCreds))
	require.NoError(t, err)

	// サーバ設定後に起動する
	go srv.Serve(l)

	conn := &mockClientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{DialCreds: clientCreds}
	// テスト用リゾルバの作成
	// リゾルバはGetServersを呼び出してサーバを解決し、サーバのアドレスでクライアントコネクションを更新する
	r := &Resolver{}
	_, err = r.Build(
		resolver.Target{
			URL: url.URL{
				Path: l.Addr().String(),
			},
		},
		conn,
		opts,
	)

	wantState := resolver.State{
		Addresses: []resolver.Address{{
			Addr:       "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {
			Addr:       "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}
	// リゾルバが2つのサーバ情報を保持していることの確認 (9001番ポートをリーダーと認識)
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	// リゾルバがサーバ情報を保持していないことの確認
	require.Equal(t, wantState, conn.state)
}

// mockGetServers は server.GetServerer インタフェースを実装する構造体。
type mockGetServers struct{}

// GetServers は DistributedLog.GetServers() のモック。
// 既知のサーバ情報の集合を返却する。
func (m *mockGetServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{{
		Id:       "leader",
		RpcAddr:  "localhost:9001",
		IsLeader: true,
	}, {
		Id:       "follower",
		RpcAddr:  "localhost:9002",
		IsLeader: false,
	}}, nil
}

// mockClientConn は resolver.ClientConn を実装する構造体。
type mockClientConn struct {
	resolver.ClientConn
	state resolver.State
}

// UpdateState はクライアントコネクションの状態更新のみ行う。
func (c *mockClientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *mockClientConn) ReportError(error) {}

func (c *mockClientConn) NewAddress([]resolver.Address) {}

func (c *mockClientConn) NewServiceConfig(string) {}

func (c *mockClientConn) ParseServiceConfig(string) *serviceconfig.ParseResult {
	return nil
}
