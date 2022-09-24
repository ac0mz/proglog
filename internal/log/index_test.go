package log

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndex はインデックス生成、書き込み、読み込み、再起動を正常と異常の観点で検証する。
func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	// ファイル作成直後はサイズが0のため読み出しエラーとなること
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	// 書き込みと読み込みの検証
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// 既存のエントリ数を超えて読み出す場合、インデックスはエラーを返す
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// クローズ後のサービス再起動において、インデックスは既存ファイルから以前の状態を再構築する
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	// 既存ファイルにエントリが書き込まれている状態で読み出す場合はエラーとならない
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
