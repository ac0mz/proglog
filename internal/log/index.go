package log

import (
	"io"
	"os"
	"syscall"
)

// インデックスエントリを構成するバイト数を定義
const (
	offWidth uint64 = 4                   // オフセット番号(レコードID)の領域
	posWidth uint64 = 8                   // ストアファイル内レコード位置の領域
	entWidth uint64 = offWidth + posWidth // インデックスエントリのサイズ
)

// index はストアファイル内の各レコードへのインデックス情報を保持する。
type index struct {
	file *os.File // 永続化されたファイル
	mmap []byte   // メモリマップされたファイル
	size uint64   // インデックスのサイズ(次にインデックスに追加されるエントリをどこに書き込むかを表す)
}

// newIndex は指定されたファイルからindexを作成する。
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// インデックスエントリの追加時にインデックスファイル内のデータ量を管理するため、現在のファイルサイズを保存
	idx.size = uint64(fi.Size())
	// ファイルサイズを最大のインデックスサイズまで空領域で増やす
	// ※一度メモリマップした領域は後からサイズ変更できないため
	// ※空領域の追加により最後のエントリがファイルの最後ではなくなるため、Closeにて切り詰め処理を実行
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// ファイルをメモリにマッピング
	if idx.mmap, err = syscall.Mmap(
		int(idx.file.Fd()),
		0,
		int(c.Segment.MaxIndexBytes),
		syscall.PROT_READ|syscall.PROT_WRITE,
		syscall.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Close はメモリマップされたデータを同期し、ファイルに永続化してグレースフルシャットダウン(※)で閉じる。
// ※進行中のタスクを終了し、データ損失が発生しないよう後処理を行い、サービスが再起動できるように準備すること。
func (i *index) Close() error {
	// メモリにマップされたファイルデータを永続化されたファイルに同期
	if err := syscall.Munmap(i.mmap); err != nil {
		return err
	}
	// 永続化されたファイルをストレージに同期
	if err := i.file.Sync(); err != nil {
		return err
	}
	// 永続化されたファイルを実際のデータ量まで切り詰めて空領域を除去し、最後のエントリをファイルの最後にする
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Read はオフセットを受け取り、ストア内の関連したオフセットとレコードの位置を返却する。
// オフセットはセグメントのベースオフセットからの相対的な値で、0は常にインデックスの最初のエントリとなる。
// -1をオフセットとして渡した場合、インデックス最後のエントリとして扱う。
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	// エントリ位置の算出
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	// オフセットと位置をデコードして、メモリマップされたファイルから読み出す
	out = enc.Uint32(i.mmap[pos : pos+offWidth])          // エントリ位置からオフセット領域末尾まで
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth]) // 現在のオフセット領域末尾からエントリ領域末尾まで
	return out, pos, nil
}

// Write は渡されたオフセットとレコード位置をインデックスに追加する。
func (i *index) Write(off uint32, pos uint64) error {
	// 空き領域のチェック
	if i.isMaxed() {
		return io.EOF
	}
	// オフセットと位置をエンコードして、メモリマップされたファイルに書き込み
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)          // 現在の書き込み位置からオフセット領域末尾まで
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos) // 現在のオフセット領域末尾からエントリ領域末尾まで
	// 次の書き込みが行われる位置を進める
	i.size += uint64(entWidth)
	return nil
}

// isMaxed はインデックスにエントリを書き込む領域が存在するかを判定する。
func (i *index) isMaxed() bool {
	// 最大のインデックスサイズより現在のファイルサイズの方が大きい場合はtrue
	return uint64(len(i.mmap)) < i.size+entWidth
}

// Name はインデックスのファイルパスを返却する。
func (i *index) Name() string {
	return i.file.Name()
}
