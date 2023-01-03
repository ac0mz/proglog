package log

import (
	"io"
	"os"
	"testing"

	api "github.com/ac0mz/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestSegment はセグメント処理の実行、および再構築時における既存の読み出しを正常・異常の観点で検証する。
// インデックスファイルとストアファイルが最大サイズに達していることの確認も行う。
func TestSegment(t *testing.T) {
	dir, err := os.MkdirTemp("", "segment-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}
	baseOff := uint64(16)

	c := Config{}
	c.Segment.MaxStoreBytes = 1024         // 検証用として十分なサイズを確保
	c.Segment.MaxIndexBytes = entWidth * 3 // 3件までのエントリサイズ (36B)

	s, err := newSegment(dir, baseOff, c)
	require.NoError(t, err)
	// インデックスが空のためbaseOffsetがnextOffsetとして設定されている
	require.Equal(t, baseOff, s.nextOffset)
	require.False(t, s.isMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		// 1件追加される度に返却値がbaseOffsetから+1ずつ増加する
		require.Equal(t, baseOff+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	// インデックスが最大であること (3件分のエントリが追加されたため)
	require.True(t, s.isMaxed())
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	require.NoError(t, s.Close())

	p, _ := proto.Marshal(want)
	c.Segment.MaxStoreBytes = uint64(len(p)+lenWidth) * 4 // 境界値検証用サイズとして92B
	c.Segment.MaxIndexBytes = 1024                        // 検証用として十分なサイズを確保
	// 既存のセグメントを再構築
	s, err = newSegment(dir, baseOff, c)
	require.NoError(t, err)
	// ストアが最大であること
	// ※再構築前におけるClose直前のAppendにてインデックス追加は失敗したが、
	// ストアは追加された状態のため、Append前のサイズ69Bに対して1レコード分の+23Bで結果92Bとなる
	// 1レコードにおける23Bの内訳は、15(バイナリワイヤ形式によるwantのサイズ) + 8(lenWidth)である
	require.True(t, s.isMaxed())
	// インデックスとストアのファイルを物理削除
	require.NoError(t, s.Remove())

	// セグメントを再構築 (各ファイルは新規作成)
	s, err = newSegment(dir, baseOff, c)
	require.NoError(t, err)
	require.False(t, s.isMaxed())
	require.NoError(t, s.Close())
}
