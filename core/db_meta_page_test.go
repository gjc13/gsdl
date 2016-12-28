package core

import (
	"testing"

	pager "github.com/gjc13/gsdl/pager"
)

func TestDbMetaToPageData(t *testing.T) {
	page := &dbMetaPage{
		FirstTableMetaPageNumber: 2,
	}
	data := page.toPageData()
	if len(data) != int(pager.PGSIZE) {
		t.Error("Wrong page size")
	}
	cp_data := append([]byte(nil), data...)
	cp_page := dbMetaPageFromPageData(0, cp_data)
	if cp_page.FirstTableMetaPageNumber != page.FirstTableMetaPageNumber {
		t.Error("Wrong recovered page, origin %d, now %d",
			page.FirstTableMetaPageNumber, cp_page.FirstTableMetaPageNumber)
	}
}
