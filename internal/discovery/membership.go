package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership は各サービスにディスカバリとクラスタメンバーシップを提供する。
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// Config はSerfを設定するための設定種別を定義する。
type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// setupSerf はSerfインスタンスの作成と設定を行い、Serfイベントを処理するハンドラを別ゴルーチンで起動する。
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String() // Serfのゴシッププロトコルで使用するIP
	config.MemberlistConfig.BindPort = addr.Port        // Serfのゴシッププロトコルで使用するポート
	m.events = make(chan serf.Event)
	config.EventCh = m.events           // ノードがクラスタに参加・離脱した時にSerfイベントを受信する手段
	config.Tags = m.Tags                // ノードの処理方法をクラスタに伝えるメタデータ(RPCアドレスや定数値など)
	config.NodeName = m.Config.NodeName // Serfクラスタ全体におけるノードの一意な識別子(未設定の場合はホスト名がデフォルト値)
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		// StartJoinAddrsフィールドは、新規ノードが既存クラスタに参加するよう設定する仕組みで用いられる。
		// クラスタ内ノードのアドレスを設定すると、Serfがノードをクラスタに参加させる。
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return err
}

// Handler はサーバがクラスタに参加・離脱したことを知る必要があるサービス内のコンポーネントを表す。
type Handler interface {
	Join(name, addr string) error
	Leave(name string) error
}

// eventHandler はSerfからイベントチャネルに送られてくるイベントを読み込んで、
// 受信した各イベントをイベントの種別に応じて処理する。
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			// Serfは複数メンバーの更新を1つのイベントにまとめて送信する可能性があるため、
			// イベントのMembersでループ処理する
			for _, member := range e.(serf.MemberEvent).Members {
				// イベント対象のノードがローカルサーバの場合、自分自身に作用することを回避
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

// handleJoin はクラスタへの参加イベントを処理する。
func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// handleJoin はクラスタからの離脱イベントを処理する。
func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}

// isLocal は指定されたSerfメンバーがローカルメンバーであるかを、メンバーの名前を確認して返却する。
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members はその時点でのクラスタにおけるSerfメンバーのスナップショットを返却する。
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave はメンバーがクラスタから離脱することを指示する。
func (m *Membership) Leave() error {
	return m.serf.Leave()
}

// logError は与えられたエラーとメッセージをロギングする。
//
// NOTE:
//
//	リーダーではないノードでクラスタを変更しようとすると、Raftはエラーとなり ErrNotLeader を返却する。
//	現在のサービスディスカバリのコードでは、すべてのハンドラエラーを重大なエラーとしてロギングしている。
//	ただし、ノードがリーダーではない場合（ErrNotLeader）はデバッグレベルでロギングするようにする。
func (m *Membership) logError(err error, msg string, member serf.Member) {
	log := m.logger.Error
	if err == raft.ErrNotLeader {
		log = m.logger.Debug
	}
	log(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}
