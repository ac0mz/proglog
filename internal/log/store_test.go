package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// テストデータ
var (
	write = []byte("hello world")
	width = uint64(len(write)) + lenWidth
)

// TestStoreAppendRead サービス再起動後に状態を回復できることを確認する。
// ストアへの追加と読み出しを検証し、ストア再作成後に再度読み出しを検証する。
func TestStoreAppendRead(t *testing.T) {
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// 初回ストア作成
	s, err := newStore(f)
	require.NoError(t, err)
	// 検証
	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// ストア再作成
	s, err = newStore(f)
	require.NoError(t, err)
	// 検証
	testRead(t, s)
}

// testAppend データ永続化メソッドの呼び出し検証ヘルパー
func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		// 書き込みバイト数とレコード開始位置の合計値が、
		// テストデータのバイト数とlenWidthの合計値と等しいことの検証
		require.Equal(t, width*i, pos+n)
	}
}

// testRead データ読み出しメソッドの呼び出し検証ヘルパー
func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		// 読み出しのデータが書き込みと等しいことの検証
		require.Equal(t, write, read)
		pos += width
	}
}

// testReadAt データ読み出しメソッドの呼び出し検証ヘルパー
func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, off)
		require.NoError(t, err)
		// 読み出したバイト数がレコード長の定数と等しいことの検証
		require.Equal(t, lenWidth, n)
		off += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, off)
		require.NoError(t, err)
		// 読みだしたデータが書き込みデータと等しいことの検証
		require.Equal(t, write, b)
		// 読み出したバイト数が1つ前に読み出したバイト数のレコード長と等しいことの検証
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

// TestStoreClose 正常にファイルをクローズすることを確認する。
func TestStoreClose(t *testing.T) {
	f, err := os.CreateTemp("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	// データ準備
	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	// Closeメソッドによるバッファ書き込み前
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	// Closeメソッドによるバッファ書き込み後
	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, beforeSize < afterSize)
}

// openFile は読み書き可能のパーミッション600でファイルを開く。
func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}
