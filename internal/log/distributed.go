package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	api "github.com/ac0mz/proglog/api/v1"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"
)

// DistributedLog は分散ログサーバが保持するログ情報を管理する。
type DistributedLog struct {
	config  Config
	log     *Log      // 単一サーバでの複製を行わないログ
	raftLog *logStore // raftで作成した分散複製ログ
	raft    *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

// setupLog はサーバのログストアを作成し、ユーザのレコードをストアに保存する。
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// setupRaft はRaftサーバの設定を行い、インスタンスを作成する。
//
// NOTE: Raftインスタンスの構成要素  ※当該メソッドの設定対象
//   - 与えられたコマンドを適用する有限ステートマシン (FSM: finite-state_machine)
//   - 上記コマンドを保存するログストア (log_store)
//   - クラスタ構成 (クラスタ内のサーバ、アドレス、現在のターム、サーバが投票した候補など重要なメタデータ) を
//     保存する安定ストア (stable_store; key-value_store; Boltを利用)
//   - データのコンパクトなスナップショットを保存するスナップショットストア (snapshot_store)
//     必要なときに効率的にデータを復旧する
//   - 他のRaftサーバと接続するために使うネットワークトランスポート
func (l *DistributedLog) setupRaft(dataDir string) (err error) {
	fsm := &fsm{log: l.log}

	logDir := filepath.Join(dataDir, "raft", "log")
	if err = os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1 // raftの要件に従い、初期オフセットを1に設定
	l.raftLog, err = newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	retain := 1 // 1つのスナップショットを保持する
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID // サーバの一意なIDで設定必須
	// 以下、テスト高速化用にタイムアウトを上書き
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		l.raftLog,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	// NOTE: 以下ブロックは不要かも。今後の正誤表を要確認。
	hasState, err := raft.HasExistingState(l.raftLog, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	// 1台目のサーバの場合、ブートストラップを実行
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Append はログにレコードを追加する。
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply はRaftのAPIにリクエストを適用し、そのレスポンスを返却する。
func (l *DistributedLog) apply(
	reqType RequestType,
	req proto.Message,
) (interface{}, error) {
	var buf bytes.Buffer // Raftが複製するレコードのデータ

	if _, err := buf.Write([]byte{byte(reqType)}); err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if _, err := buf.Write(b); err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout) // レコードを複製し、リーダーのログにレコード追加
	// 結果（エラーか正常終了か）が分かるまで待機
	// ※エラーのパターンは、Raftが処理するコマンドに時間が掛かっている場合、サーバがシャットダウンした場合
	if future.Error() != nil {
		return nil, err
	}
	// FSMのApplyメソッドの結果を返却
	res := future.Response()
	// Raftは単一の値を返却するため、型アサーションで検査
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read はサーバのログからオフセットで指定されたレコードを読み出す。
// 緩やかな一貫性 (relaxed consistency) のため、Raftを経由せずに読み出し操作を行う。
//
//	NOTE:
//	 強い一貫性 (strong consistency) が必要な場合、読み出しは書き込みに対して
//	 最新でなければならないためRaftを経由する必要があるが、読み出し効率が悪くなり性能が落ちる。
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

var _ raft.FSM = (*fsm)(nil)

// fsm は有限ステートマシン (finite-state machine) として操作する対象のログを管理する。
type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Apply はログエントリをコミット後にRaftから呼び出される。
func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	// リクエスト種別でどのコマンドを実行する (ロジックを含む対応メソッドを呼び出す) かを切り分け
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

// applyAppend はローカルのログにレコードを追加する。
func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	if err := proto.Unmarshal(b, &req); err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot は定期的にRaftから呼び出され、状態 (FSMのログ) の point-in-time snapshot を取得する。
//
// 設定した SnapshotInterval, SnapshotThreshold に従ってRaftから呼び出される。
//   - SnapshotInterval: Raftがsnapshotを取得すべきかを検査する間隔。デフォルトは2分。
//   - SnapshotThreshold: 新規snapshotの作成前に、最後のsnapshotから何個のログを取得するか。デフォルトは8192個。
//
// NOTE: 当該スナップショットの2つの目的
//   - 1つはRaftがすでに適用したコマンドのログを保存しないよう、Raftのログをコンパクトにする
//   - リーダーがログ全体を何度も複製させずに、Raftが新規でサーバを起動できるようにする
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist はRaftから呼び出され、状態 (FSMのログ) をスナップショットストアに保存する。
//
//	NOTE:
//	 当該サービスではスナップショットストアとしてファイルを使用するため、
//	 スナップショットが完了すると、Raftのすべてのログデータを含むファイルを取得できる。
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release はスナップショットが終了すると、Raftから呼び出される。
func (s *snapshot) Release() {}

// Restore はスナップショットからFSMを復元するためにRaftから呼び出される。
//
//	NOTE:
//	 あるサーバが失われた後に新たなサーバを追加した場合、失ったサーバのFSMを復元する状況において
//	 FSMの状態がリーダーの複製された状態と一致するよう、既存の状態を破棄する必要がある。
func (f *fsm) Restore(snapshot io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(snapshot, b)
		if err == io.EOF {
			break // すべて読み出し終えたらループを抜ける
		} else if err != nil {
			return err
		}
		size := int64(enc.Uint64(b))
		if _, err = io.CopyN(&buf, snapshot, size); err != nil {
			return err
		}

		// 元のレコードを復元
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			// 1件目のレコードの場合、初期オフセットとしてレコードのオフセットを設定し、
			// 既存の状態を破棄 (初期オフセットを用いて新規セグメントを作成)
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

// FirstIndex はRaftから呼び出され、最古のオフセット（Raftではインデックスと呼ぶ）を取得する。
func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

// LastIndex はRaftから呼び出され、最新のオフセット（Raftではインデックスと呼ぶ）を取得する。
func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

// GetLog はRaftから呼び出され、indexを元にレコードを取得し、outに値を設定する。
func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

// StoreLog はRaftから呼び出され、ログにレコードを追加する。
func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

// StoreLogs はRaftから呼び出され、ログにレコードを追加する。
func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange は古いレコードを削除する。
//
//	NOTE:
//	 本来DeleteRangeメソッドはオフセット間のレコードを削除する。
//	 古いレコードやスナップショットに保存されているレコードを削除するためにRaftから呼び出される。
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// StreamLayer はRaftサーバと接続するにあたって低レベルなストリーム抽象化を提供するための、
// トランスポートのStreamLayerインタフェースを満たすメソッドを実装する。
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config // サーバ間の暗号化通信における受信コネクションを受け入れるためのTLS設定
	peerTLSConfig   *tls.Config // サーバ間の暗号化通信における送信コネクションを作成するためのTLS設定
}

func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// Dial はRaftクラスタ内における他サーバへの新たな発信コネクションを作成する。
//
//	NOTE:
//	 サーバ接続の際、コネクション種別を識別するためにRaftRPCバイトを書き込み、
//	 ログのgRPCリクエストと同じポートでRaftを多重化できる。
//	 ストリームレイヤをピアTLSで設定することで、TLSクライアント側の接続が行われる。
func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	// Raft RPC であることを特定する
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, nil
}

// Accept はDialメソッドに対応し、入ってくるコネクションを受け入れ、
// コネクション種別を識別するバイトを読み出し、サーバ側のTLS接続を作成する。
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

// Close はリスナーをクローズする。
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr はリスナーのアドレスを返却する。
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
