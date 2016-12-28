package page_map

import (
	"fmt"
)

type FreePageMap struct {
	selfPgNumber uint32
	freeMap      []byte
	numFree      int
	size         int
}

func (pageMap *FreePageMap) NextFreePageNumber() uint32 {
	for i := 0; i < pageMap.Size(); i++ {
		if !pageMap.GetAtOffset(i) {
			return pageMap.selfPgNumber + uint32(i)
		}
	}
	return 0
}

func (pageMap *FreePageMap) assertInRange(pgNumber uint32) {
	pgOffset := int(pgNumber) - int(pageMap.selfPgNumber)
	if pgOffset < 0 || pgOffset > pageMap.Size() {
		panic(fmt.Sprintf("Page %d not in free page map %d", pgNumber, pageMap.selfPgNumber))
	}
}

func (pageMap *FreePageMap) Set(pgNumber uint32) {
	pageMap.assertInRange(pgNumber)
	pgOffset := pgNumber - pageMap.selfPgNumber
	i := pgOffset / 8
	offset := pgOffset - i<<3
	if !pageMap.GetAtOffset(int(pgOffset)) {
		pageMap.numFree--
	}
	pageMap.freeMap[i] |= (byte(1 << offset))
}

func (pageMap *FreePageMap) UnSet(pgNumber uint32) {
	pageMap.assertInRange(pgNumber)
	pgOffset := pgNumber - pageMap.selfPgNumber
	i := pgOffset / 8
	offset := pgOffset - i<<3
	if pageMap.GetAtOffset(int(pgOffset)) {
		pageMap.numFree++
	}
	pageMap.freeMap[i] &= ^(byte(1 << offset))
}

func (pageMap *FreePageMap) NumFree() int {
	return pageMap.numFree
}

func (pageMap *FreePageMap) Get(pgNumber uint32) bool {
	pageMap.assertInRange(pgNumber)
	return pageMap.GetAtOffset(int(pgNumber - pageMap.selfPgNumber))
}

func (pageMap *FreePageMap) GetAtOffset(pgOffset int) bool {
	i := pgOffset / 8
	offset := uint32(pgOffset - i<<3)
	return pageMap.freeMap[i]&(1<<offset) != 0
}

func (pageMap *FreePageMap) Size() int {
	return pageMap.size
}

func (pageMap *FreePageMap) SerializeSize() int {
	return len(pageMap.freeMap)
}

func (pageMap *FreePageMap) Serialize() []byte {
	return pageMap.freeMap
}

func MakeFreePageMap(pgNumber uint32, size int) *FreePageMap {
	mapSize := (size + 7) / 8
	return &FreePageMap{
		selfPgNumber: pgNumber,
		freeMap:      make([]byte, mapSize),
		numFree:      size,
		size:         size,
	}
}

func Deserialize(pgNumber uint32, size int, data []byte) *FreePageMap {
	pageMap := &FreePageMap{
		selfPgNumber: pgNumber,
		freeMap:      data,
		size:         size,
		numFree:      0,
	}
	for i := 0; i < size; i++ {
		if !pageMap.GetAtOffset(i) {
			pageMap.numFree++
		}
	}
	return pageMap
}
