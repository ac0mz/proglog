package loadbalance

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

const (
	methodNameProduce = "/log.vX.Log/Produce"
	methodNameConsume = "/log.vX.Log/Consume"
)

// Test_Picker_NoSubConnAvailable はリゾルバがサーバを発見し、利用可能なサブコネクションで
// ピッカーの状態を更新する前に、ピッカーが利用不可エラーを返すことを検証する。
//
//	NOTE:
//	 balancer.ErrNoSubConnAvailable はピッカーが利用可能なサブコネクションを持つまで、
//	 クライアントのRPCをブロックするようにgRPCに支持を出す。
func Test_Picker_NoSubConnAvailable(t *testing.T) {
	picker := &Picker{}
	methods := []string{
		methodNameProduce,
		methodNameConsume,
	}
	for _, method := range methods {
		info := balancer.PickInfo{FullMethodName: method}
		result, err := picker.Pick(info)
		require.Equal(t, balancer.ErrNoSubConnAvailable, err)
		require.Nil(t, result.SubConn)
	}
}

// Test_Picker_ProducesToLeader はピッカーがProduce呼び出しのために
// リーダーのサブコネクションを選択することを検証する。
func Test_Picker_ProducesToLeader(t *testing.T) {
	picker, subConns := setupTest(t)
	info := balancer.PickInfo{
		FullMethodName: methodNameProduce,
	}
	for _ = range make([]struct{}, 5) {
		gotPick, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[0], gotPick.SubConn)
	}
}

// Test_Picker_ConsumesFromFollowers はピッカーがConsume呼び出しのために
// ラウンドロビンでフォロワーのサブコネクションを選択することを検証する。
func Test_Picker_ConsumesFromFollowers(t *testing.T) {
	picker, subConns := setupTest(t)
	info := balancer.PickInfo{
		FullMethodName: methodNameConsume,
	}
	for i := range make([]struct{}, 5) {
		gotPick, err := picker.Pick(info)
		require.NoError(t, err)
		require.Equal(t, subConns[i%2+1], gotPick.SubConn)
	}
}

// setupTest はモックのサブコネクションを持つテスト用ピッカーを作成する。
// リゾルバの集合と同じ属性を持つアドレスを含んだデータでピッカーをBuildする。
func setupTest(t *testing.T) (*Picker, []*mockSubConn) {
	t.Helper()

	var subConns []*mockSubConn
	buildInfo := base.PickerBuildInfo{
		ReadySCs: make(map[balancer.SubConn]base.SubConnInfo),
	}
	for i := 0; i < 3; i++ {
		sc := &mockSubConn{}
		addr := resolver.Address{
			Attributes: attributes.New("is_leader", i == 0),
		}
		// 0
		sc.UpdateAddresses([]resolver.Address{addr})
		buildInfo.ReadySCs[sc] = base.SubConnInfo{Address: addr}
		subConns = append(subConns, sc)
	}
	picker := &Picker{}
	picker.Build(buildInfo)
	return picker, subConns
}

// mockSubConn は balancer.SubConn を実装する構造体。
type mockSubConn struct {
	addrs []resolver.Address
}

func (s *mockSubConn) UpdateAddresses(addrs []resolver.Address) {
	s.addrs = addrs
}

func (s *mockSubConn) Connect() {}

var _ balancer.SubConn = (*mockSubConn)(nil)
