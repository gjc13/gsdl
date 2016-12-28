package core

import "errors"

var (
	// errors
	ERR_ALLOC              = errors.New("cannot alloc page")
	ERR_EMPTY              = errors.New("empty")
	ERR_NOT_FOUND          = errors.New("not found")
	ERR_OVERLAPPED         = errors.New("element overlapped")
	ERR_SEARCH_OVERFLOWED  = errors.New("search overflowed")
	ERR_SEARCH_UNDERFLOWED = errors.New("search underflowed")
	ERR_END_ITER           = errors.New("End of iter")
	ERR_NIL                = errors.New("Filed cannot be nil")
)
