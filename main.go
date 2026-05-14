package main

import (
	"context"
	"encoding/binary"
	"errors"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"time"

	"github.com/cloudwego/netpoll"
	"github.com/gobwas/ws"
)

const maxPayload = 4096

type connState struct {
	upgraded bool
}

type ctxKey struct{}

var stateKey = ctxKey{}

var errNotEnough = errors.New("not enough data")

func init() {
	// I/O 极重的网关场景:每个 P 配一个 poller
	netpoll.SetNumLoops(runtime.GOMAXPROCS(0))
}

func main() {
	go func() {
		log.Println("🔬 Pprof on :6063")
		_ = http.ListenAndServe(":6063", nil)
	}()

	listener, err := netpoll.CreateListener("tcp", ":8080")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	eventLoop, err := netpoll.NewEventLoop(
		onRequest,
		netpoll.WithOnPrepare(onPrepare),
		netpoll.WithReadTimeout(30*time.Second),
		netpoll.WithWriteTimeout(10*time.Second),
		netpoll.WithIdleTimeout(10*time.Minute),
	)
	if err != nil {
		log.Fatalf("event loop: %v", err)
	}

	log.Println("🚀 nocopy WS gateway on :8080")
	if err := eventLoop.Serve(listener); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func onPrepare(connection netpoll.Connection) context.Context {
	return context.WithValue(context.Background(), stateKey, &connState{})
}

func onRequest(ctx context.Context, connection netpoll.Connection) error {
	state := ctx.Value(stateKey).(*connState)

	// 握手仍走 gobwas/ws.Upgrade(每条连接只发生一次,阻塞可接受)
	if !state.upgraded {
		if _, err := ws.Upgrade(connection); err != nil {
			return err
		}
		state.upgraded = true
	}

	// 尽可能消费当前 buffer 里所有完整帧
	for {
		err := processOneFrame(connection)
		if errors.Is(err, errNotEnough) {
			return nil // 数据不够,等下次 OnRequest 被触发
		}
		if err != nil {
			return err // 关连接
		}
	}
}

// processOneFrame 零拷贝处理一个 WebSocket 帧
func processOneFrame(connection netpoll.Connection) error {
	reader := connection.Reader()

	// 至少 2 字节才能开始解析
	if reader.Len() < 2 {
		return errNotEnough
	}

	// === 第 1 步:Peek 头部(不消费)===
	// WS 头最大 14 字节:2 基础 + 8 扩展长度 + 4 mask key
	peekLen := min(reader.Len(), 14)
	hdrBytes, err := reader.Peek(peekLen)
	if err != nil {
		return err
	}

	// === 第 2 步:解析头部 ===
	headerSize, payloadLen, masked, maskKey, opcode, fin, perr := parseWSHeader(hdrBytes)
	if errors.Is(perr, errNotEnough) {
		return errNotEnough
	}
	if perr != nil {
		return perr
	}

	if payloadLen > maxPayload {
		return errors.New("payload too large")
	}

	// === 第 3 步:确认整帧都到齐了 ===
	totalFrame := headerSize + int(payloadLen)
	if reader.Len() < totalFrame {
		return errNotEnough
	}

	// === 第 4 步:消费掉头部 ===
	if err := reader.Skip(headerSize); err != nil {
		return err
	}

	// === 第 5 步:零拷贝拿到 payload 切片 ===
	// 这个 slice 直接指向 netpoll 内部 LinkBuffer 的内存,没有拷贝
	payload, err := reader.Next(int(payloadLen))
	if err != nil {
		return err
	}

	// === 第 6 步:就地 unmask(slice 可写)===
	if masked {
		ws.Cipher(payload, maskKey, 0)
	}

	// === 第 7 步:构造回程头部(服务端发往客户端不带 mask)===
	// 头部最长 10 字节,用栈数组,完全无堆分配
	var respHdr [10]byte
	respHdrLen := buildServerHeader(respHdr[:], opcode, fin, payloadLen)

	// === 第 8 步:写出 ===
	writer := connection.Writer()

	// 头部小,WriteBinary 一次拷贝可忽略
	if _, err := writer.WriteBinary(respHdr[:respHdrLen]); err != nil {
		return err
	}

	// payload 走 WriteDirect:零拷贝把读 buffer 的切片"挂"到写 buffer 上
	// Flush 之后 reader 的内存才会被回收,所以 Flush 不能省、不能延后到下一帧
	if err := writer.WriteDirect(payload, 0); err != nil {
		return err
	}

	// Flush:底层 writev(2),header + payload 合并成一次 syscall 发出
	return writer.Flush()
}

// parseWSHeader 手工解析 WebSocket 帧头(RFC 6455 §5.2)
//
// 帧格式:
//
//	 0                   1                   2                   3
//	 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
//	+-+-+-+-+-------+-+-------------+-------------------------------+
//	|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
//	|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
//	|N|V|V|V|       |S|             |   (if payload len==126/127)   |
//	| |1|2|3|       |K|             |                               |
//	+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
//	|     Extended payload length continued, if payload len == 127  |
//	+ - - - - - - - - - - - - - - - +-------------------------------+
//	|                               |Masking-key, if MASK set to 1  |
//	+-------------------------------+-------------------------------+
func parseWSHeader(buf []byte) (
	headerSize int,
	payloadLen uint64,
	masked bool,
	maskKey [4]byte,
	opcode ws.OpCode,
	fin bool,
	err error,
) {
	if len(buf) < 2 {
		err = errNotEnough
		return
	}

	b0 := buf[0]
	b1 := buf[1]

	fin = b0&0x80 != 0
	// RSV1-3 (0x70) 忽略;若开启 permessage-deflate 扩展,RSV1 会被置位
	// 但帧结构本身不变,echo 网关原样转发即可
	opcode = ws.OpCode(b0 & 0x0F)

	masked = b1&0x80 != 0
	length7 := b1 & 0x7F

	switch {
	case length7 < 126:
		payloadLen = uint64(length7)
		headerSize = 2
	case length7 == 126:
		if len(buf) < 4 {
			err = errNotEnough
			return
		}
		payloadLen = uint64(binary.BigEndian.Uint16(buf[2:4]))
		headerSize = 4
	case length7 == 127:
		if len(buf) < 10 {
			err = errNotEnough
			return
		}
		payloadLen = binary.BigEndian.Uint64(buf[2:10])
		headerSize = 10
	}

	if masked {
		if len(buf) < headerSize+4 {
			err = errNotEnough
			return
		}
		copy(maskKey[:], buf[headerSize:headerSize+4])
		headerSize += 4
	}

	return
}

// buildServerHeader 构造服务端→客户端的帧头(MUST NOT mask,RFC 6455 §5.1)
func buildServerHeader(out []byte, opcode ws.OpCode, fin bool, payloadLen uint64) int {
	b0 := byte(opcode) & 0x0F
	if fin {
		b0 |= 0x80
	}
	out[0] = b0

	switch {
	case payloadLen < 126:
		out[1] = byte(payloadLen)
		return 2
	case payloadLen <= 0xFFFF:
		out[1] = 126
		binary.BigEndian.PutUint16(out[2:4], uint16(payloadLen))
		return 4
	default:
		out[1] = 127
		binary.BigEndian.PutUint64(out[2:10], payloadLen)
		return 10
	}
}
