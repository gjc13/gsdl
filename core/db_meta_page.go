package core

import (
	"bytes"
	"encoding/binary"

	utils "github.com/gjc13/gsdl/utils"
)

type dbMetaPage struct {
	PageNumber               uint32
	FirstTableMetaPageNumber uint32
}

func (page *dbMetaPage) toPageData() []byte {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, page); err != nil {
		panic("Failed to serialize Db meta page")
	}
	data := buf.Bytes()
	return utils.PadToPage(data)
}

func dbMetaPageFromPageData(pgNumber uint32, data []byte) *dbMetaPage {
	if pgNumber != 0 {
		panic("Wrong db meta page number")
	}
	buf := bytes.NewBuffer(data)
	var page dbMetaPage
	if err := binary.Read(buf, binary.LittleEndian, &page); err != nil {
		panic("Failed to deserialize Db meta page")
	}
	return &page
}
