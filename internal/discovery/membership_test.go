package discovery

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/serf/serf"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

// TestMembership は複数のサーバを持つクラスタを設定し、Membershipがメンバーシップに参加した全てのサーバを返し、
// サーバがクラスタから離脱した後に更新することを検証する。
func TestMembership(t *testing.T) {
	m, handler := setupMember(t, nil) // クラスタを形成する最初のメンバー
	m, _ = setupMember(t, m)          // 参加イベントの発生(1回目)
	m, _ = setupMember(t, m)          // 参加イベントの発生(2回目)
	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && // 参加イベントが2回発生
			len(m[0].Members()) == 3 && // クラスタに登録されているメンバー数が3
			len(handler.leaves) == 0 // 離脱イベントが未発生
	}, 3*time.Second, 250*time.Millisecond)

	// 最後のメンバーをクラスタから離脱させる
	require.NoError(t, m[2].Leave())

	require.Eventually(t, func() bool {
		return len(handler.joins) == 2 && // 参加イベントが2回発生
			len(m[0].Members()) == 3 && // クラスタに登録されているメンバー数が3
			m[0].Members()[2].Status == serf.StatusLeft && // グレースフルに離脱済
			len(handler.leaves) == 1 // 離脱イベントが1回発生
	}, 3*time.Second, 250*time.Millisecond)

	// 最後のメンバー(添字の最終値)で離脱イベントが発生したことの検証
	require.Equal(t, "2", <-handler.leaves)
}

// setupMember は空きポート番号で新しいメンバーを設定し、ノード名としてメンバーの長さを用いて一意にする。
// メンバーの長さは、このメンバーがクラスタの最初のメンバーなのか参加するクラスタがあるのかを表す。
func setupMember(t *testing.T, members []*Membership) ([]*Membership, *handler) {
	id := len(members)
	ports := dynaport.Get(1)
	addr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
	tags := map[string]string{"rpc_addr": addr}
	c := Config{
		NodeName: fmt.Sprintf("%d", id),
		BindAddr: addr,
		Tags:     tags,
	}
	h := &handler{}
	if len(members) == 0 {
		h.joins = make(chan map[string]string, 3)
		h.leaves = make(chan string, 3)
	} else {
		c.StartJoinAddrs = []string{members[0].BindAddr}
	}
	// メンバーの生成、およびクラスタへの追加
	m, err := New(h, c)
	require.NoError(t, err)
	members = append(members, m)
	return members, h
}

// handler はHandlerインタフェースを実装したメソッドの呼び出し情報を保持する。
type handler struct {
	joins  chan map[string]string
	leaves chan string
}

// Join はHandlerインタフェースのモック。
// どのIDとアドレスで何回呼び出されたのかをキャプチャする。
func (h *handler) Join(id, addr string) error {
	if h.joins != nil {
		h.joins <- map[string]string{
			"id":   id,
			"addr": addr,
		}
	}
	return nil
}

// Leave はHandlerインタフェースのモック。
// どのIDで何回呼び出されたのかをキャプチャする。
func (h *handler) Leave(id string) error {
	if h.leaves != nil {
		h.leaves <- id
	}
	return nil
}
