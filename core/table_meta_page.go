package core

import (
	"bytes"
	"encoding/binary"

	utils "github.com/gjc13/gsdl/utils"
)

type tableMetaPage struct {
	PgNumber              uint32
	TableName             string
	ColumnNames           []string
	RowInfo               *RowMeta
	FirstDataPgNumber     uint32
	FieldIndexPgNumbers   []uint32
	NextTableMetaPgNumber uint32
	Dropped               uint8
}

func (page *tableMetaPage) dropped() bool {
	return page.Dropped > 0
}

func (page *tableMetaPage) toPageData() []byte {
	buf := new(bytes.Buffer)
	var err error
	if err = binary.Write(buf, binary.LittleEndian, int32(len(page.FieldIndexPgNumbers))); err != nil {
		panic("Failed to serialize")
	}
	if err = binary.Write(buf, binary.LittleEndian, append([]byte(page.TableName), 0)); err != nil {
		panic("Failed to serialize")
	}
	for _, name := range page.ColumnNames {
		if err = binary.Write(buf, binary.LittleEndian, append([]byte(name), 0)); err != nil {
			panic("Failed to serialize")
		}
	}
	if err = binary.Write(buf, binary.LittleEndian, page.RowInfo.ClusterFieldId); err != nil {
		panic("Failed to serialize")
	}
	for i := 0; i < len(page.FieldIndexPgNumbers); i++ {
		if err = binary.Write(buf, binary.LittleEndian, page.RowInfo.FieldMetas[i]); err != nil {
			panic("Failed to serialize")
		}
	}
	if err = binary.Write(buf, binary.LittleEndian, page.FirstDataPgNumber); err != nil {
		panic("Failed to serialize")
	}
	for i := 0; i < len(page.FieldIndexPgNumbers); i++ {
		if err = binary.Write(buf, binary.LittleEndian, page.FieldIndexPgNumbers[i]); err != nil {
			panic("Failed to serialize")
		}
	}
	if err = binary.Write(buf, binary.LittleEndian, page.NextTableMetaPgNumber); err != nil {
		panic("Failed to serialize")
	}
	if err = binary.Write(buf, binary.LittleEndian, page.Dropped); err != nil {
		panic("Failed to serialize")
	}
	data := buf.Bytes()
	return utils.PadToPage(data)
}

func tableMetaPageFromData(pgNumber uint32, data []byte) *tableMetaPage {
	buf := bytes.NewBuffer(data)
	var numRows int32
	var err error
	if err = binary.Read(buf, binary.LittleEndian, &numRows); err != nil {
		panic("Failed to deserialize table meta page")
	}
	page := tableMetaPage{
		PgNumber: pgNumber,
		RowInfo: &RowMeta{
			FieldMetas: make([]FieldMeta, 0, numRows),
		},
		ColumnNames:         make([]string, 0, numRows),
		FieldIndexPgNumbers: make([]uint32, 0, numRows),
	}
	if page.TableName, err = buf.ReadString(0); err != nil {
		panic("Failed to deserialize table meta page")
	}
	page.TableName = utils.ShrinkString(page.TableName)
	for i := 0; i < int(numRows); i++ {
		var name string
		if name, err = buf.ReadString(0); err != nil {
			panic("Failed to deserialize table meta page")
		}
		page.ColumnNames = append(page.ColumnNames, utils.ShrinkString(name))
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.RowInfo.ClusterFieldId); err != nil {
		panic("Failed to deserialize table meta page")
	}
	for i := 0; i < int(numRows); i++ {
		var m FieldMeta
		if err = binary.Read(buf, binary.LittleEndian, &m); err != nil {
			panic("Failed to deserialize table meta page")
		}
		page.RowInfo.FieldMetas = append(page.RowInfo.FieldMetas, m)
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.FirstDataPgNumber); err != nil {
		panic("Failed to deserialize table meta page")
	}
	for i := 0; i < int(numRows); i++ {
		var n uint32
		if err = binary.Read(buf, binary.LittleEndian, &n); err != nil {
			panic("Failed to deserialize table meta page")
		}
		page.FieldIndexPgNumbers = append(page.FieldIndexPgNumbers, n)
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.NextTableMetaPgNumber); err != nil {
		panic("Failed to deserialize table meta page")
	}
	if err = binary.Read(buf, binary.LittleEndian, &page.Dropped); err != nil {
		panic("Failed to deserialize table meta page")
	}
	return &page
}
