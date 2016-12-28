package core

import (
	"bytes"
	"encoding/binary"
	"fmt"

	utils "github.com/gjc13/gsdl/utils"
)

type indexPage struct {
	PgNumber     uint32
	Children     Elems
	PrevPgNumber uint32
	NextPgNumber uint32
	Internal     uint8
}

func (page *indexPage) toPageData() []byte {
	buf := new(bytes.Buffer)
	var err error
	var numChildren int32 = int32(len(page.Children))
	if err = binary.Write(buf, binary.LittleEndian, numChildren); err != nil {
		panic("Failed to serialize")
	}
	for _, elem := range page.Children {
		if err = binary.Write(buf, binary.LittleEndian, elem.Key); err != nil {
			panic("Failed to serialize")
		}
		if err = binary.Write(buf, binary.LittleEndian, elem.PgNumber); err != nil {
			panic("Failed to serialize")
		}
	}
	if err = binary.Write(buf, binary.LittleEndian, page.PrevPgNumber); err != nil {
		panic("Failed to serialize")
	}
	if err = binary.Write(buf, binary.LittleEndian, page.NextPgNumber); err != nil {
		panic("Failed to serialize")
	}
	if err = binary.Write(buf, binary.LittleEndian, page.Internal); err != nil {
		panic("Failed to serialize")
	}
	return utils.PadToPage(buf.Bytes())
}

func (page *indexPage) isInternal() bool {
	return page.Internal != 0
}

func (page *indexPage) Key() Key {
	return page.Children[0].Key
}

func (page *indexPage) Last() Key {
	return page.Children[len(page.Children)-1].Key
}

func (page *indexPage) insertElem(elem Elem, maxDegree int, allowOverlap bool) error {
	newChildren, err := page.Children.insert(elem, maxDegree, allowOverlap)
	if err != nil {
		return err
	}
	page.Children = newChildren
	return nil
}

func (page *indexPage) deleteElem(key Key, maxDegree int) bool {
	newChildren, ok := page.Children.delete(key, maxDegree)
	if !ok {
		return false
	}
	page.Children = newChildren
	return true
}

func indexPageFromData(pgNumber uint32, data []byte) *indexPage {
	buf := bytes.NewBuffer(data)
	var page indexPage
	var numChildren int32
	var err error
	if err = binary.Read(buf, binary.LittleEndian, &numChildren); err != nil {
		panic("Failed to deserialize index page")
	}
	page.Children = make([]Elem, 0, numChildren)
	for i := 0; i < int(numChildren); i++ {
		var elem Elem
		if err = binary.Read(buf, binary.LittleEndian, &elem.Key); err != nil {
			panic("Failed to deserialize index page")
		}
		if err = binary.Read(buf, binary.LittleEndian, &elem.PgNumber); err != nil {
			panic("Failed to deserialize index page")
		}
		page.Children = append(page.Children, elem)
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.PrevPgNumber); err != nil {
		panic("Failed to deserialize index page")
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.NextPgNumber); err != nil {
		panic("Failed to deserialize index page")
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.Internal); err != nil {
		panic("Failed to deserialize index page")
	}
	page.PgNumber = pgNumber
	return &page
}

func (page *indexPage) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("pg %d (prev %d, next %d, interal %d):[",
		page.PgNumber, page.PrevPgNumber, page.NextPgNumber, page.Internal))
	for _, elem := range page.Children {
		buf.WriteString(fmt.Sprintf("(%d: %d), ", elem.Key, elem.PgNumber))
	}
	buf.WriteString("]")
	return buf.String()
}
