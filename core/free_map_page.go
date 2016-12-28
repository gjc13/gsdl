package core

import (
	pager "github.com/gjc13/gsdl/pager"
	page_map "github.com/gjc13/gsdl/utils/page_map"
)

type freeMapPage struct {
	pgNumber    uint32
	freePageMap *page_map.FreePageMap
}

func (page *freeMapPage) toPageData() []byte {
	return page.freePageMap.Serialize()
}

func freeMapPageFromPageData(pgNumber uint32, data []byte) *freeMapPage {
	return &freeMapPage{
		pgNumber,
		page_map.Deserialize(pgNumber, int(pager.PGSIZE)*8, data),
	}
}
