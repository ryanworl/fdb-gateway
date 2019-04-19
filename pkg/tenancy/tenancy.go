package tenancy

import (
	"crypto/subtle"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type TenantAuthorizer struct {
	database  fdb.Database
	directory directory.DirectorySubspace
}

func NewTenantAuthorizer(db fdb.Database, dir directory.DirectorySubspace) *TenantAuthorizer {
	return &TenantAuthorizer{db, dir}
}

func (t *TenantAuthorizer) Authorize(directory []byte, user []byte, password []byte) (bool, error) {
	value, err := t.database.Transact(func(tn fdb.Transaction) (interface{}, error) {
		k := t.directory.Pack(tuple.Tuple{"tenants", directory, user, password})
		v, err := tn.Get(k).Get()

		// Yes, this leaks length information. Don't use short passwords!
		if err == nil && subtle.ConstantTimeCompare(v, password) == 1 {
			return true, nil
		}

		return false, err
	})

	authorized := value.(bool)

	return authorized, err
}
