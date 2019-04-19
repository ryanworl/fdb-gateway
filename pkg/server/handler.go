package server

import (
	"bytes"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

	"github.com/ryanworl/fdb-gateway/pkg/tenancy"

	"github.com/tidwall/redcon"
)

const (
	BEGIN        = "BEGIN"
	CANCEL       = "CANCEL"
	COMMIT       = "COMMIT"
	READ_VERSION = "READ_VERSION"
	OK           = "OK"
)

const (
	CHROOT_ERROR = "ERR must chroot into a directory first"
)

type Handler struct {
	database   fdb.Database
	authorizer *tenancy.TenantAuthorizer
	root       directory.Directory
}

func NewHandler(db fdb.Database, auth *tenancy.TenantAuthorizer, root directory.Directory) *Handler {
	return &Handler{
		database:   db,
		authorizer: auth,
		root:       root,
	}
}

func arity(arity int, conn redcon.Conn, cmd redcon.Command) bool {
	if len(cmd.Args) != arity {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	}

	return len(cmd.Args) == arity
}

func chrooted(conn redcon.Conn) bool {
	ctx, _ := ctx(conn)

	return ctx.Directory != nil
}

func ctx(conn redcon.Conn) (*ConnCtx, *fdb.Transaction) {
	ctx := conn.Context().(*ConnCtx)

	return ctx, ctx.Transaction
}

func wrap(cb func(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace)) func(conn redcon.Conn, cmd redcon.Command) {
	return func(conn redcon.Conn, cmd redcon.Command) {
		chrooted := chrooted(conn)
		if !chrooted {
			conn.WriteError(CHROOT_ERROR)
			return
		}

		ctx, tn := ctx(conn)
		if tn == nil {
			conn.WriteError("ERR no open transaction")
			return
		}

		cb(conn, cmd, tn, ctx.Directory)
	}
}

func (h *Handler) begin(conn redcon.Conn, cmd redcon.Command) {
	chrooted := chrooted(conn)
	if !chrooted {
		conn.WriteError(CHROOT_ERROR)
		return
	}

	ctx, tn := ctx(conn)
	if tn != nil {
		conn.WriteError("ERR transaction already open")
		return
	}

	newTn, err := h.database.CreateTransaction()
	if err != nil {
		conn.WriteError("ERR cannot begin transaction")
		return
	}

	ctx.Transaction = &newTn

	readVersion := newTn.GetReadVersion().MustGet()

	conn.WriteArray(2)
	conn.WriteString(READ_VERSION)
	conn.WriteInt(int(readVersion))
}

func (h *Handler) cancel(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	ctx, _ := ctx(conn)

	tn.Cancel()

	ctx.Transaction = nil

	conn.WriteArray(1)
	conn.WriteString(CANCEL)
}

func (h *Handler) commit(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	ctx, _ := ctx(conn)

	err := tn.Commit().Get()
	if err != nil {
		conn.WriteArray(2)
		conn.WriteString(CANCEL)
		conn.WriteError(fmt.Sprint(err))
		return
	}

	version, err := tn.GetCommittedVersion()
	if err != nil {
		conn.WriteError(fmt.Sprint(err))
		return
	}

	ctx.Transaction = nil

	conn.WriteArray(2)
	conn.WriteString(COMMIT)
	conn.WriteInt(int(version))
}

func (h *Handler) ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (h *Handler) quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString(OK)
	conn.Close()
}

func (h *Handler) set(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	if !arity(3, conn, cmd) {
		return
	}

	key := s.Pack(tuple.Tuple{cmd.Args[1]})
	tn.Set(key, cmd.Args[2])

	conn.WriteString(OK)
}

func (h *Handler) get(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	if !arity(2, conn, cmd) {
		return
	}

	key := s.Pack(tuple.Tuple{cmd.Args[1]})
	val, err := tn.Get(key).Get()
	if err != nil {
		conn.WriteError(fmt.Sprint(err))
	}

	if val == nil {
		conn.WriteNull()
	} else {
		conn.WriteBulk(val)
	}
}

func (h *Handler) get_range(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	if !arity(3, conn, cmd) {
		return
	}

	begin := s.Pack(tuple.Tuple{cmd.Args[1]})
	end := s.Pack(tuple.Tuple{cmd.Args[2]})
	r := fdb.KeyRange{Begin: begin, End: end}

	it := tn.GetRange(r, fdb.RangeOptions{}).Iterator()

	prefix := s.Bytes()

	var err error
	results := make([]fdb.KeyValue, 0, 1)
	for it.Advance() {
		v, err := it.Get()
		v.Key = v.Key[len(prefix)+1 : len(v.Key)-1]

		if err == nil {
			results = append(results, v)
		}
	}

	if err != nil {
		conn.WriteError(fmt.Sprint(err))
		return
	}

	conn.WriteArray(len(results))
	for _, kv := range results {
		conn.WriteArray(2)
		conn.WriteBulk(kv.Key)
		conn.WriteBulk(kv.Value)
	}
}

func (h *Handler) clear(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	if !arity(2, conn, cmd) {
		return
	}

	key := s.Pack(tuple.Tuple{cmd.Args[1]})
	tn.Clear(key)

	conn.WriteString(OK)
}

func (h *Handler) clear_range(conn redcon.Conn, cmd redcon.Command, tn *fdb.Transaction, s directory.DirectorySubspace) {
	if !arity(3, conn, cmd) {
		return
	}

	begin := s.Pack(tuple.Tuple{cmd.Args[1]})
	end := s.Pack(tuple.Tuple{cmd.Args[2]})
	r := fdb.KeyRange{Begin: begin, End: end}

	tn.ClearRange(r)

	conn.WriteString(OK)
}

func (h *Handler) chroot(conn redcon.Conn, cmd redcon.Command) {
	if !arity(4, conn, cmd) {
		return
	}

	directory := cmd.Args[1]
	username := cmd.Args[2]
	password := cmd.Args[3]

	authorized, err := h.authorizer.Authorize(directory, username, password)
	if err != nil {
		conn.WriteError(fmt.Sprint(err))
		return
	}

	if !authorized {
		conn.WriteError("ERR unauthorized")
		return
	}

	subspace, err := h.root.CreateOrOpen(h.database, []string{string(directory)}, []byte("gateway"))
	if err != nil {
		conn.WriteError("ERR cannot open subspace")
		return
	}

	ctx, _ := ctx(conn)
	ctx.Directory = subspace

	conn.WriteString(OK)
}

func printable(d []byte) string {
	buf := new(bytes.Buffer)
	for _, b := range d {
		if b >= 32 && b < 127 && b != '\\' {
			buf.WriteByte(b)
			continue
		}
		if b == '\\' {
			buf.WriteString("\\\\")
			continue
		}
		buf.WriteString(fmt.Sprintf("\\x%02x", b))
	}
	return buf.String()
}
