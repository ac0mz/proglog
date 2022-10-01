package log

import (
	"context"
	"sync"

	api "github.com/ac0mz/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Replicator はgRPCを用いて他サーバに接続する。
type Replicator struct {
	DialOptions []grpc.DialOption // gRPCクライアントを設定するためのオプション
	LocalServer api.LogClient     // Produceメソッドにより他サーバから読みだしたメッセージのコピー保存用

	logger *zap.Logger

	mu      sync.Mutex
	servers map[string]chan struct{} // サーバアドレスとチャネルのマップ(サーバからのレプリケーション停止用)
	closed  bool
	close   chan struct{}
}

// Join は指定されたサーバをレプリケーション対象のサーバのリストに追加し、
// レプリケーション処理を実行するゴルーチンを起動する。
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		// 既にレプリケーション済のためスキップ
		return nil
	}
	r.servers[name] = make(chan struct{})

	go r.replicate(addr, r.servers[name])

	return nil
}

// replicate は見つかったサーバのログをストリームから読み出し、ローカルサーバに書き込んでコピーを保存する。
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	// gRPCクライアントの作成
	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			// サーバ上のすべてのログを読み出すストリームをオープン
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		// サーバが故障するかクラスタを離れた場合はゴルーチンを終了し、それまではログを複製し続ける
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave はサーバがクラスタから離脱する際に、レプリケート対象サーバのリストから離脱するサーバを削除し、
// そのサーバに関連付けられたチャネルを閉じる。
// チャネルを閉じることで、サーバからのレプリケーションを停止するよう、
// replicateメソッドを実行しているゴルーチンに通知する。
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// init はサーバのマップをデフォルト値(有用なゼロ値)で遅延初期化する。
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close は既存のサーバのレプリケーションを停止する。
// レプリケータを閉じることで、クラスタに参加する新たなサーバのデータをレプリケーションしないようにする。
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// logError はエラーを記録する。
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
