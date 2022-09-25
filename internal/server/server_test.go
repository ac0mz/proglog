package server

import (
	"context"
	"net"
	"os"
	"testing"

	api "github.com/ac0mz/proglog/api/v1"
	"github.com/ac0mz/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestServer はテストケース一覧を定義し、各テストケースを実行する。
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, client api.LogClient, cfg *Config){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, cfg)
		})
	}
}

// setupTest は各テストケースを設定するヘルパー関数である。
func setupTest(t *testing.T, fn func(*Config)) (cli api.LogClient, cfg *Config, teardown func()) {
	t.Helper()

	// サーバが動作するローカルのアドレスに対してリスナーを作成(0番ポート指定により空きポートが割り当てられる)
	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// 暗号化されていないコネクションを使うオプションの定義、およびサーバを呼び出すクライアントの作成
	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)
	cli = api.NewLogClient(cc)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	// sutの作成
	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	if fn != nil {
		fn(cfg)
	}
	// サーバの作成
	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)
	go func() {
		// Serveメソッドは、l.Acceptメソッドが失敗しない限り処理が戻ってこないブロッキング呼び出しのため、
		// ゴルーチンでリクエスト処理を開始する
		server.Serve(l)
	}()

	return cli, cfg, func() {
		// 後処理
		cc.Close()
		server.Stop()
		l.Close()
		clog.Remove()
	}
}

// testProduceConsume は書き込みと読み込みの検証を行う。
func testProduceConsume(t *testing.T, cli api.LogClient, cnf *Config) {
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
func testProduceConsumeStream(t *testing.T, cli api.LogClient, cnf *Config) {
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
func testConsumePastBoundary(t *testing.T, cli api.LogClient, cnf *Config) {
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
