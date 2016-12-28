package core

import (
	pager "github.com/gjc13/gsdl/pager"
)

type DbContext struct {
	transaction pager.Transactioner
	metaPage    *dbMetaPage
}
