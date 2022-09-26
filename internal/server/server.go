package server

import (
	"context"
	"time"

	api "github.com/ac0mz/proglog/api/v1"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (*grpc.Server, error) {
	// サーバが各RPCのサブジェクトを識別して認可処理を開始できるようミドルウェアを設定
	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_auth.StreamServerInterceptor(authenticate),
			),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(
				grpc_auth.UnaryServerInterceptor(authenticate),
			),
		))

	gsrv := grpc.NewServer(grpcOpts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// Produce はクライアントがサーバにログを書き込むリクエストを処理する。
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {

	// 書き込みの認可
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume はクライアントがサーバからログを読み出すリクエストを処理する。
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {

	// 読み出しの認可
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream は双方向ストリーミングRPCの実装である。
// クライアントは複数リクエストをサーバにストリーミングし、サーバは各リクエストの成否をクライアントに伝える。
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream はサーバ側ストリーミングRPCの実装である。
// クライアントはサーバにログ内のどのレコードを読み出すか指示し、
// サーバはそのレコード以降のすべて(未書き込み含む)のレコードをストリーミングする。
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest,
	stream api.Log_ConsumeStreamServer) error {

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				// 新たなレコードが入力されるまでポーリングしているためスリープを挟む
				time.Sleep(time.Second)
				continue
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// authenticate はクライアント証明書からサブジェクトを読み取り、RPCのコンテキストに書き込むミドルウェア。
// ミドルウェア(別名インタセプタ)により、各RPC呼び出しの実行を途中で変更する。
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

// subject はクライアント証明書のサブジェクトを返却する。
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
