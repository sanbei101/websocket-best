package main

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func setupTestServer(b *testing.B) net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("Failed to listen: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsGatewayHandler)

	server := &http.Server{Handler: mux}
	go func() {
		server.Serve(ln)
	}()

	return ln
}

func BenchmarkWSEcho(b *testing.B) {
	ln := setupTestServer(b)
	defer ln.Close()

	wsURL := "ws://" + ln.Addr().String() + "/ws"
	payload := []byte("hello, high-performance websocket gateway! testing extreme low latency...")

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, _, _, err := ws.Dial(context.Background(), wsURL)
		if err != nil {
			b.Fatalf("Dial error: %v", err)
		}
		defer conn.Close()
		for pb.Next() {
			err = wsutil.WriteClientText(conn, payload)
			if err != nil {
				b.Fatalf("Write error: %v", err)
			}

			reply, err := wsutil.ReadServerText(conn)
			if err != nil {
				b.Fatalf("Read error: %v", err)
			}

			if len(reply) != len(payload) {
				b.Fatalf("Payload length mismatch: got %d, want %d", len(reply), len(payload))
			}
		}
	})
}
