package main

import (
	"log"

	"github.com/ac0mz/proglog/internal/server"
)

// main はサーバを作成して起動するエントリーポイントである
func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())
}
