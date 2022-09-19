package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian // レコードサイズとインデックスエントリの永続化用エンコーディング
)

const (
	lenWidth = 8 // レコード長の格納用バイト数を定義
)

// store はファイルを保持し、ファイルにバイトを追加および読み出しを行うAPIを備える。
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore は与えられたファイルに対するstoreを作成する。
func newStore(f *os.File) (*store, error) {
	// ファイル名をキーにファイル情報を取得
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// ファイルの現在のサイズを取得
	// (サービス再起動等により既存ファイルからstoreを再作成する場合にこのサイズ情報を利用)
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append は与えられたバイトデータをストアに永続化し、レコードサイズとレコード開始位置を返却する。
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size

	// レコード読み出し時に何バイト読めば良いか分かるようにするため、レコードの長さを書き込み
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	// システムコールの数を減らしてパフォーマンスを改善するために、
	// バッファ付きライターにバイトデータを書き込み、書き込んだバイト数を取得
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// 書き込んだバイト数とレコード長の合計値をサイズとする
	w += lenWidth
	s.size += uint64(w)
	// レコードサイズ、およびストアがファイル内で保持するレコード開始位置(※)を返却
	// ※このレコードに関連するインデックスエントリを作成する際に、セグメントは当該レコード位置を利用する
	return uint64(w), pos, nil
}

// Read は指定された位置に格納されているレコードを返却する。
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// バッファ内に残っているファイル未永続化のデータを書き込み
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	// レコード全体を読み取るために必要なバイト数を取得
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	// レコードを取得
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt はストアにおけるファイルのオフセット位置から始まるバイトデータを読み込み、バイト数を返却する。
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close はファイルをクローズする。ただしクローズ前にバッファされたデータを永続化する。
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
