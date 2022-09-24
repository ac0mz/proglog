package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/ac0mz/proglog/api/v1"
)

// Log はセグメントの集合体であるsegmentsと書き込み対象のactiveSegmentを保持する。
// Dirにセグメントを保存する。
type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

// NewLog はLogインスタンスを作成する。引数のConfigの値が未指定の場合はデフォルト値を設定する。
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// setup はセグメントの準備を行う。
func (l *Log) setup() error {
	// ストレージ上に存在するセグメントの一覧を取得
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		// 各セグメントのファイル名からベースオフセットの値を導出してスライスに格納
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		// セグメントのスライスを古い順でソートする準備として、ベースオフセットをソート
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		// 既存セグメント(インデックスとストア)を作成
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffsetsはインデックスとストアの2つの重複を含んでいるため、重複しているものをスキップ
		i++
	}
	if l.segments == nil {
		// 既存セグメントが存在しない場合、最初のセグメントを作成
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment は新たなセグメントを作成し、アクティブセグメントとする。
// 新規作成されたセグメントはセグメントのスライス末尾に追加される。
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Append はログにレコードを追加する。
// アクティブセグメントが最大サイズに到達していた場合、新たなセグメントを作成する。
func (l *Log) Append(record *api.Record) (uint64, error) {
	// NOTE: 当該実装を最適化すれば、セグメント毎にロックを獲得することも可能
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOffset, err := l.highestOffset()
	if err != nil {
		return 0, err
	}
	if l.activeSegment.isMaxed() {
		err = l.newSegment(highestOffset + 1)
		if err != nil {
			return 0, err
		}
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	return off, nil
}

// Read は指定されたオフセットに保存されているレコードをセグメントから読み出す。
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			// 指定されたオフセットがbaseOffset以上、かつnextOffsetより小さい最初のレコード
			// ※古い順でセグメントが並んでおり、セグメントのbaseOffsetがセグメント内の最小オフセットのため
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

// Close はセグメントをすべて閉じる。
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove はログを閉じて、そのログのデータを削除する。
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset はログを削除して、置き換える新たなログを作成する。
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset は最古のオフセットを返却する。
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset は現時点で最新のオフセット(nextOffsetの直前)を返却する。
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.highestOffset()
}

// highestOffset は現時点で最新のオフセットを返却する。
func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate は最大オフセットがlowestよりも小さいセグメントをすべて削除する。
// ディスク容量を空けるためのメンテナンス用途として使用が想定される。
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// Reader はログ全体を読み込むためのio.Readerを返却する。
// 合意形成の連携においてスナップショット、およびログの復旧ををサポートする場合に利用する。
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	// セグメントのストアを連結
	return io.MultiReader(readers...)
}

// originReader は次の理由からストアを保持する。
// 1. io.Readerインタフェースを満たし、それをio.MultiReader呼び出し時に渡すため。
// 2. ストアの最初から読み込みを開始し、そのファイル全体を読み込むことを保証するため。
type originReader struct {
	*store
	off int64
}

func (o originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
