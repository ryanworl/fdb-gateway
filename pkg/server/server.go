package server

import (
	"log"
	"sync/atomic"

	"github.com/ryanworl/fdb-gateway/pkg/respmux"
	"github.com/ryanworl/fdb-gateway/pkg/tenancy"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"

	"github.com/tidwall/redcon"
)

type ConnCtx struct {
	Id          int64
	Transaction *fdb.Transaction
	Directory   directory.DirectorySubspace
}

type Server struct {
	database   fdb.Database
	authorizer *tenancy.TenantAuthorizer
	root       directory.Directory
	idCounter  int64
}

func NewServer(db fdb.Database, auth *tenancy.TenantAuthorizer, root directory.Directory) *Server {
	return &Server{db, auth, root, 0}
}

func (s *Server) Accept(conn redcon.Conn) bool {
	id := atomic.AddInt64(&s.idCounter, 1)

	ctx := &ConnCtx{id, nil, nil}

	conn.SetContext(ctx)

	return true
}

func (s *Server) Closed(conn redcon.Conn, err error) {
	log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
}

func (s *Server) Serve(addr string) error {
	handler := NewHandler(s.database, s.authorizer, s.root)

	mux := respmux.NewRESPMux()
	mux.HandleFunc("ping", handler.ping)
	mux.HandleFunc("quit", handler.quit)

	mux.HandleFunc("chroot", handler.chroot)

	mux.HandleFunc("begin", handler.begin)
	mux.HandleFunc("cancel", wrap(handler.cancel))
	mux.HandleFunc("commit", wrap(handler.commit))

	mux.HandleFunc("get", wrap(handler.get))
	mux.HandleFunc("get_range", wrap(handler.get_range))
	mux.HandleFunc("set", wrap(handler.set))
	mux.HandleFunc("clear", wrap(handler.clear))
	mux.HandleFunc("clear_range", wrap(handler.clear_range))

	return redcon.ListenAndServe(addr, mux.ServeRESP, s.Accept, s.Closed)
}
