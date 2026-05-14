package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"

	"github.com/gobwas/ws"
)

var bufPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096+16))
	},
}

func main() {
	go func() {
		log.Println("🔬 Pprof monitor started on :6063")
		if err := http.ListenAndServe(":6063", nil); err != nil {
			log.Fatalf("Pprof server failed: %v", err)
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsGatewayHandler)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	log.Println("🚀 High-Performance WS Gateway started on :8080")
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func wsGatewayHandler(w http.ResponseWriter, r *http.Request) {
	conn, _, _, err := ws.UpgradeHTTP(r, w)
	if err != nil {
		return
	}

	go handleConnection(conn)
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		header, err := ws.ReadHeader(conn)
		if err != nil {
			break
		}
		if header.Length > 4096 {
			log.Println("Payload too large")
			break
		}

		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

		buf.Grow(int(header.Length))
		payload := buf.Bytes()[:header.Length]

		if _, err := io.ReadFull(conn, payload); err != nil {
			bufPool.Put(buf)
			break
		}

		if header.Masked {
			ws.Cipher(payload, header.Mask, 0)
		}

		respHeader := ws.Header{
			Fin:    true,
			OpCode: ws.OpText,
			Length: int64(len(payload)),
		}

		buf.Reset()
		if err := ws.WriteHeader(buf, respHeader); err != nil {
			bufPool.Put(buf)
			break
		}

		buf.Write(payload)

		if _, err := conn.Write(buf.Bytes()); err != nil {
			bufPool.Put(buf)
			break
		}

		bufPool.Put(buf)
	}
}
