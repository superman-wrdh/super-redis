package server

import (
	"context"
	"net"
)

type Handler struct {
}

// MakeHandler creates a Handler instance
func MakeHandler() *Handler {
	return &Handler{}
}
func (h *Handler) Handle(ctx context.Context, conn net.Conn) {
	// TODO
}

// Close stops handler
func (h *Handler) Close() error {
	// TODO
	return nil
}
