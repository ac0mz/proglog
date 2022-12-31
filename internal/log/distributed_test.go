package log_test

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	api "github.com/ac0mz/proglog/api/v1"
	"github.com/ac0mz/proglog/internal/log"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

// Test_MultipleNodes は分散ログの検証を行う。
// テストの動作環境によってはtimeoutの設定値により失敗する場合があるが、
// XxxTimeout や Sleep の秒数に余裕を持たせるよう設定変更すると成功する。
func Test_MultipleNodes(t *testing.T) {
	var logs []*log.DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	// 3つのサーバから構成されるクラスタを設定
	for i := 0; i < nodeCount; i++ {
		dataDir, err := os.MkdirTemp("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dataDir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := log.Config{}
		config.Raft.StreamLayer = log.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		// Raftが素早くリーダーを選出するよう、デフォルトのタイムアウトを短く設定する
		config.Raft.HeartbeatTimeout = 100 * time.Millisecond
		config.Raft.ElectionTimeout = 100 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 100 * time.Millisecond
		config.Raft.CommitTimeout = 50 * time.Millisecond

		if i == 0 {
			// クラスタをブートストラップしてリーダになる
			config.Raft.Bootstrap = true
		}

		l, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			// クラスタに追加
			err = logs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}
		logs = append(logs, l)
	}

	// レプリケーションの検証
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}
	for _, record := range records {
		// リーダーのサーバにレコードを追加
		off, err := logs[0].Append(record)
		require.NoError(t, err)

		// 500ミリ秒以内に処理が正常終了すること
		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				// Raftがリーダー側に追加されたレコードをフォロワー側に複製した結果のレコードを読み取り
				got, err := logs[j].Read(off)
				if err != nil {
					return false
				}
				record.Offset = off
				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	err := logs[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	off, err := logs[0].Append(&api.Record{
		Value: []byte("third"),
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// リーダーがクラスタから離脱したサーバへのレプリケーションを停止していることの検証
	record, err := logs[1].Read(off)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, record)

	// 既存サーバへのレプリケーションが継続していることの検証
	record, err = logs[2].Read(off)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, off, record.Offset)
}
