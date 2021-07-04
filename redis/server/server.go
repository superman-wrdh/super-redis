package server

import "C"
import (
	"context"
	"io"
	"net"
	"strings"
	"super-redis/core"
	"super-redis/interface/db"
	"super-redis/lib/logger"
	"super-redis/lib/sync/atomic"
	"super-redis/redis/connection"
	"super-redis/redis/parser"
	"super-redis/redis/reply"
	"sync"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown\r\n")
)

type Handler struct {
	activeConn sync.Map
	db         db.DB
	closing    atomic.Boolean
}

func (h *Handler) closeClient(client *connection.Connection) {
	_ = client.Close()
	//h.db.AfterClientClose(client)
	h.activeConn.Delete(client)
}

// 创建一个redis handler实例
func MakeHandler() *Handler {

	var db db.DB
	db = core.MakeDB()

	return &Handler{
		db: db,
	}
}

//处理请求 执行redis命令
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	// 储存当前连接
	h.activeConn.Store(client, 1)
	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// connection closed
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol err
			errReply := reply.MakeErrReply(payload.Err.Error())
			err := client.Write(errReply.ToBytes())
			if err != nil {
				h.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := payload.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		result := h.db.Exec(client, r.Args)
		if result != nil {
			_ = client.Write(result.ToBytes())
		} else {
			_ = client.Write(unknownErrReplyBytes)
		}
	}
}

// 关闭handler
func (h *Handler) Close() error {
	// TODO
	return nil
}
