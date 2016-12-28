package core

import (
	"io"

	pager "github.com/gjc13/gsdl/pager"
	page_map "github.com/gjc13/gsdl/utils/page_map"
)

func createFreeMapPage(wt *pager.WriteTransaction, pgNumber uint32) error {
	pageMap := &freeMapPage{
		pgNumber:    pgNumber,
		freePageMap: page_map.MakeFreePageMap(pgNumber, int(pager.PGSIZE)*8),
	}
	pageMap.freePageMap.Set(pgNumber)
	return wt.WritePage(pgNumber, pageMap.toPageData())
}

func allocPage(ctx *DbContext) (uint32, error) {
	wt, ok := ctx.transaction.(*pager.WriteTransaction)
	if !ok {
		panic("Transaction error: Not write transaction when allocating page")
	}
	var pgNumber uint32 = 1
	for {
		data, err := wt.ReadPage(pgNumber)
		if err == io.EOF {
			err1 := createFreeMapPage(wt, pgNumber)
			if err1 != nil {
				return 0, err1
			}
			return allocPage(ctx)
		} else if err != nil {
			return 0, err
		}
		fmp := freeMapPageFromPageData(pgNumber, data)
		allocPgNumber := fmp.freePageMap.NextFreePageNumber()
		if allocPgNumber == 0 {
			pgNumber += uint32(fmp.freePageMap.Size())
		} else {
			fmp.freePageMap.Set(allocPgNumber)
			return allocPgNumber, wt.WritePage(pgNumber, fmp.toPageData())
		}
	}
}

func freePage(ctx *DbContext, pgNumber uint32) error {
	wt, ok := ctx.transaction.(*pager.WriteTransaction)
	if !ok {
		panic("Transaction error: Not write transaction when allocating page")
	}
	if pgNumber == 0 {
		panic("Cannot free header page")
	}
	freeMapPgNumber := pgNumber/uint32(pager.PGSIZE*8) + 1
	if pgNumber == freeMapPgNumber {
		panic("Cannot free pagmap page")
	}
	data, err := wt.ReadPage(freeMapPgNumber)
	if err != nil {
		return err
	}
	fmp := freeMapPageFromPageData(freeMapPgNumber, data)
	fmp.freePageMap.UnSet(pgNumber)
	return wt.WritePage(freeMapPgNumber, fmp.toPageData())
}
