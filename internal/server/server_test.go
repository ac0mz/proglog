package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/ac0mz/proglog/api/v1"
	"github.com/ac0mz/proglog/internal/auth"
	"github.com/ac0mz/proglog/internal/config"
	"github.com/ac0mz/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// TestServer はテストケース一覧を定義し、各テストケースを実行する。
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T,
		rootCli api.LogClient, nobodyCli api.LogClient, cfg *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, cfg)
		})
	}
}

// setupTest は各テストケースを設定するヘルパー関数である。
// サーバとクライアントのコネクションがTLS暗号化されるよう設定する。
func setupTest(t *testing.T, fn func(*Config)) (
	rootCli api.LogClient, nobodyCli api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// サーバが動作するローカルのアドレスに対してリスナーを作成(0番ポート指定により空きポートが割り当てられる)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// クライアントのTLS認証情報に、クライアントのRoot CA(サーバ確認用)として独自のCAを使うよう設定
	newCli := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		require.NoError(t, err)
		cli := api.NewLogClient(conn)
		return conn, cli, opts
	}

	var rootConn *grpc.ClientConn // 書き込みと読み出しが許可されたユーザのコネクション
	rootConn, rootCli, _ = newCli(config.RootClientCertFile, config.RootClientKeyFile)
	var nobodyConn *grpc.ClientConn // 未許可であるユーザのコネクション
	nobodyConn, nobodyCli, _ = newCli(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	// sutの作成
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	cfg = &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}

	// サーバの証明書と鍵を解析し、サーバのTLS認証情報を設定
	srvTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	srvCreds := credentials.NewTLS(srvTLSConfig)
	// サーバのTLS認証情報をオプションとして指定し、gRPCサーバを作成
	server, err := NewGRPCServer(cfg, grpc.Creds(srvCreds))
	require.NoError(t, err)
	go func() {
		// Serveメソッドは、l.Acceptメソッドが失敗しない限り処理が戻ってこないブロッキング呼び出しのため、
		// ゴルーチンでリクエスト処理を開始する
		server.Serve(l)
	}()

	return rootCli, nobodyCli, cfg, func() {
		// 後処理
		rootConn.Close()
		nobodyConn.Close()
		server.Stop()
		l.Close()
		clog.Remove()
	}
}

// testProduceConsume は書き込みと読み込みの検証を行う。
func testProduceConsume(t *testing.T, cli, _ api.LogClient, cnf *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produce, err := cli.Produce(ctx, &api.ProduceRequest{Record: want})
	require.NoError(t, err)
	want.Offset = produce.Offset

	consume, err := cli.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

// testProduceConsumeStream はストリームで書き込みと読み出しの検証を行う。
func testProduceConsumeStream(t *testing.T, cli, _ api.LogClient, cnf *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		// 書き込みの検証
		stream, err := cli.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv() // ストリームにおけるリクエストのレシーブ
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("want: %d, got offset: %d", offset, res.Offset)
			}
		}
	}

	{
		// 読み込みの検証
		stream, err := cli.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv() // ストリームにおけるリクエストのレシーブ
			require.NoError(t, err)
			require.Equal(t, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			}, res.Record)
		}
	}
}

// testConsumePastBoundary はクライアントがログの境界を超えて読み出す場合、エラーとなることを検証する。
func testConsumePastBoundary(t *testing.T, cli, _ api.LogClient, cnf *Config) {
	ctx := context.Background()

	// 1件目のレコード書き込み
	produce, err := cli.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)

	// 2件目のオフセット(存在しないレコード)を設定
	consume, err := cli.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	got := status.Code(err)
	if want != got {
		t.Fatalf("want: %v, got err: %v", want, got)
	}
}

// testUnauthorized はサーバにクライアントが拒否されることを検証する。
func testUnauthorized(t *testing.T, _, cli api.LogClient, cnf *Config) {
	ctx := context.Background()
	produce, err := cli.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}
	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if wantCode != gotCode {
		t.Fatalf("want code: %d, got code: %d", wantCode, gotCode)
	}

	consume, err := cli.Consume(ctx, &api.ConsumeRequest{Offset: 0})
	if consume != nil {
		t.Fatalf("consume response should be nil")
	}
	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if wantCode != gotCode {
		t.Fatalf("want code: %d, got code: %d", wantCode, gotCode)
	}
}
