package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/ac0mz/proglog/api/v1"

	"google.golang.org/protobuf/proto"
)

// segment はストアとインデックスの操作を統合するために、それぞれのポインタを保持する。
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64 // base:相対的なオフセット計算用, next:新規レコード追加時のオフセット
	config                 Config // セグメントサイズにおける最大を比較して検知するための制限値
}

// newSegment はsegmentを生成して返却する。
// 現在のアクティブセグメントが最大サイズになった場合など、新たなセグメントを作成する際にログから呼び出される。
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	// ストアファイルを開いて、セグメントにポインタを設定
	// 第2引数には論理和でファイルモードフラグを指定(O_RDWRは必須)
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		// ファイルが存在しない場合はO_CREATE、
		// かつストアファイルの場合は書き込み時にOSがファイルに永続化するようO_APPENDを渡す
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	// インデックスファイルを開いて、セグメントにポインタを設定
	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		// インデックスの場合はメモリマップされたファイルを用いる(ストレージに永続化しない)ためO_APPENDは不要
		os.O_RDWR|os.O_CREATE,
		0600,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	// 設定対象である次に追加されるオフセットを評価
	if off, _, err := s.index.Read(-1); err != nil {
		// インデックスが空の場合、ベースオフセットを次のオフセットとする
		s.nextOffset = baseOffset
	} else {
		// インデックスに少なくとも1つのエントリが存在する場合、ベースオフセットと相対オフセットの和に1を加算
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append はセグメントにレコードを書き込み、新たに追加されたレコードのオフセットを返却する。
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		// インデックスのオフセットは、ベースオフセットに対する相対的な値のため減算で求める
		uint32(s.nextOffset-s.baseOffset),
		pos,
	); err != nil {
		// WARNING: s.store.Append(p)で追加されたレコードはゴミとして残ったままとなる
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read は指定されたオフセットのレコードを返却する。
func (s *segment) Read(off uint64) (*api.Record, error) {
	// 絶対オフセットから算出した相対オフセットを引数として渡して、インデックスエントリを取得
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	// インデックスから取得した位置を使用して、ストア内のレコードからデータを読み出し
	b, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(b, record)
	return record, nil
}

// isMaxed はセグメントが最大サイズに達したか(ストアまたはインデックスへの書き込みが一杯になったか)を判定する。
// 長いレコードであればストアにおけるバイト数の上限に達しやすく、
// 短いレコードを多数書き込んでいればインデックスにおけるバイト数の上限に達しやすい。
func (s *segment) isMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

// Remove はセグメントを閉じて、インデックスファイルとストアファイルを物理削除する。
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close はセグメントで保持しているインデックスとストアを閉じる。
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
