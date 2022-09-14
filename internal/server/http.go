package server

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// NewHTTPServer はサーバのアドレスを受け取り、APIエンドポイントとハンドラーを設定した*http.Serverを返す
func NewHTTPServer(addr string) *http.Server {
	httpsrv := newHTTPServer()
	r := mux.NewRouter()
	r.HandleFunc("/", httpsrv.handleProduce).Methods("POST")
	r.HandleFunc("/", httpsrv.handleConsume).Methods("GET")
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: NewLog(),
	}
}

// ProduceRequest はログに追加されるレコードを保持する
type ProduceRequest struct {
	Record Record `json:"record"`
}

// ProduceResponse はログに格納したレコードのオフセットがどの位置かを保持する
type ProduceResponse struct {
	Offset uint64 `json:"offset"`
}

// ConsumeRequest は読み出されるレコード位置のオフセットを保持する
type ConsumeRequest struct {
	Offset uint64 `json:"offset"`
}

// ConsumeResponse は読み出したレコードを保持する
type ConsumeResponse struct {
	Record Record `json:"record"`
}

// handleProduce はログにレコードを保存、および保存した際のオフセットをレスポンスに書き込む
func (s *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	off, err := s.Log.Append(req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ProduceResponse{Offset: off}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// handleConsume はログからレコードを読み出し、レスポンスに書き込む
func (s *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// リクエストのデコード
	var req ConsumeRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// ログの読み出し
	record, err := s.Log.Read(req.Offset)
	if err == ErrOffsetNotFound {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// レスポンスの書き込み
	res := ConsumeResponse{Record: record}
	err = json.NewEncoder(w).Encode(&res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
