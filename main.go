package main

import (
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"syscall"

	"github.com/gobwas/ws"
)

var (
	bufPool = sync.Pool{
		New: func() any {
			return make([]byte, 4096)
		},
	}
)

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

	optimizeSocket(conn)

	go handleConnection(conn)
}

func optimizeSocket(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}

	tcpConn.SetNoDelay(true)
	tcpConn.SetKeepAlive(true)

	rawConn, err := tcpConn.SyscallConn()
	if err == nil {
		rawConn.Control(func(fd uintptr) {
			syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, 4096)
			syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096)
		})
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	buf := bufPool.Get().([]byte)
	defer bufPool.Put(buf)

	for {
		header, err := ws.ReadHeader(conn)
		if err != nil {
			break
		}

		payload := buf[:header.Length]
		_, err = conn.Read(payload)
		if err != nil {
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

		ws.WriteHeader(conn, respHeader)
		conn.Write(payload)
	}
}
