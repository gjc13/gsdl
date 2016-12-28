package frontend

import "errors"

var (
	// errors
	ERR_CREATE    = errors.New("Cannot create db")
	ERR_STATEMENT = errors.New("Statement Error")
	ERR_FIELD     = errors.New("Field type mismatch")
	ERR_NODB      = errors.New("No such db")
	ERR_NOTABLE   = errors.New("No such table")
	ERR_NOCOLUMN  = errors.New("No such column")
	ERR_INTERNAL  = errors.New("Internal error")
	ERR_PRIM_KEY  = errors.New("No primary key")
)
