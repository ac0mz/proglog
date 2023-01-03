package loadbalance

import (
	"strings"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var _ base.PickerBuilder = (*Picker)(nil)

// Picker はRPCをバランスさせる処理 (リゾルバが発見したサーバアドレスの中から各RPCを処理するサーバを選択) を行う。
// Consume, ConsumeStream のRPCをフォロワーサーバに、Produce, ProduceStream のRPCをリーダーサーバに送信する。
//
//	NOTE:
//	 ピッカーの役割として呼び出しの送信先決定を行うが、gRPCにはデフォルトのバランサ (※) があるため、今回は独自実装が不要となる。
//	 ※サブコネクションを管理し、接続状態を収集および集約する balancer.Balancer のこと。
type Picker struct {
	mu        sync.RWMutex
	leader    balancer.SubConn
	followers []balancer.SubConn
	current   uint64
}

// Build は引数のサブコネクションから取得したフォロワーの集合を設定したピッカーを生成する。
//
//	NOTE:
//	 gRPCは当メソッドにサブコネクションのマップと、それらサブコネクションに関する情報を渡してピッカーを生成する。
func (p *Picker) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	p.mu.Lock()
	defer p.mu.Unlock()

	var followers []balancer.SubConn
	for sc, scInfo := range buildInfo.ReadySCs {
		isLeader := scInfo.Address.Attributes.Value("is_leader").(bool)
		if isLeader {
			p.leader = sc
			continue
		}
		followers = append(followers, sc)
	}
	p.followers = followers
	return p
}

var _ balancer.Picker = (*Picker)(nil)

// Pick はRPC呼び出しを行うべきサブコネクションを含む情報を返却する。
//
//	NOTE:
//	 balancer.PickInfo は Ctx フィールドを持っており、ヘッダーのメタデータがある場合はここから読み取る。
//	 balancer.PickResult のオプションとして、Doneコールバック関数を設定可能。
//	 この関数には、RPCエラー, trailerのメタデータ, サーバとの間で送受信されたバイト数があったかどうかを含む情報が渡される。
func (p *Picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var result balancer.PickResult
	if strings.Contains(info.FullMethodName, "Produce") || len(p.followers) == 0 {
		result.SubConn = p.leader
	} else if strings.Contains(info.FullMethodName, "Consume") {
		// フォロワー間でRPC呼び出しをバランスさせる
		result.SubConn = p.nextFollower()
	}
	if result.SubConn == nil {
		return result, balancer.ErrNoSubConnAvailable
	}
	return result, nil
}

// nextFollower は次のフォロワーをラウンドロビン方式で選択して返却する。
func (p *Picker) nextFollower() balancer.SubConn {
	cur := atomic.AddUint64(&p.current, uint64(1))
	len := uint64(len(p.followers))
	idx := int(cur % len)
	return p.followers[idx]
}

func init() {
	balancer.Register(
		base.NewBalancerBuilder(Name, &Picker{}, base.Config{}),
	)
}
