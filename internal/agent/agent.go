package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/ac0mz/proglog/internal/auth"
	"github.com/ac0mz/proglog/internal/discovery"
	"github.com/ac0mz/proglog/internal/log"
	"github.com/ac0mz/proglog/internal/server"
	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux" // 様々なプロトコルに対応した汎用Multiplexer
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent はすべてのサービスインスタンス上で動作し、すべての異なるコンポーネントを設定して接続する。
type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdownLock sync.Mutex
}

// Config はAgentで保持するコンポーネントのパラメータを構成する。
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool
}

// RPCAddr はRPCアドレスを返却する。
func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New はAgentを作成し、コンポーネントを設定する一連のメソッドを実行する。
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config: config,
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// setupMux はRPCアドレスにRaftとgRPCの両方の接続を受け付けるリスナーを作成し、
// そのリスナーでmuxを作成する。
// muxはリスナーからの接続を受け付け、設定されたルールに基づいてコネクションを識別する。
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

// setupLog は分散ログのRaftが当システムの多重化リスナーを使うよう設定し、分散ログの設定と作成を行う。
func (a *Agent) setupLog() error {
	// Raftコネクションの識別用マッチャールールをmuxに設定
	// ルールにマッチした場合、Raftがコネクション処理できるよう、muxはraftLnリスナー用コネクションを返却
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		// log.StreamLayer.Dialメソッドで書き込んだRaftコネクションの発信バイトと一致しているかを返却
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(
		a.Config.DataDir,
		logConfig,
	)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log,
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	grpcLn := a.mux.Match(cmux.Any())
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return nil
}

// setupMembership はDistributedLogをハンドラとして指定し、メンバーシップを作成する。
// メンバーシップは、サーバがクラスタに参加・離脱する際にDistributedLogへ伝える。
// Raftにより、DistributedLogは連携されたレプリケーションを処理する。
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// Shutdown は実行中のエージェントを終了する。
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true

	shutdown := []func() error{
		a.membership.Leave, // メンバーシップから離脱することで、ディスカバリのイベント受信を停止
		func() error {
			a.server.GracefulStop() // グレースフルにサーバを停止
			return nil
		},
		a.log.Close, // ログを閉じる
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
