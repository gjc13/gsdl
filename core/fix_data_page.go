package core

import (
	"bytes"
	"encoding/binary"
	"errors"

	pager "github.com/gjc13/gsdl/pager"
	utils "github.com/gjc13/gsdl/utils"
)

type fixDataPage struct {
	pgNumber     uint32
	nextPgNumber uint32
	prevPgNumber uint32
	numRows      uint32
	meta         *RowMeta
	data         []byte
}

func (page *fixDataPage) firstNonNullKeyField() interface{} {
	fieldId := int(page.meta.ClusterFieldId)
	for i := 0; i < int(page.numRows); i++ {
		if k := page.getRowAt(i)[fieldId]; k != nil {
			return k
		}
	}
	return nil
}

func (page *fixDataPage) firstKeyField() interface{} {
	fieldId := int(page.meta.ClusterFieldId)
	return page.getRowAt(0)[fieldId]
}

func (page *fixDataPage) lastKeyField() interface{} {
	fieldId := int(page.meta.ClusterFieldId)
	return page.getRowAt(int(page.numRows) - 1)[fieldId]
	return nil
}

func (page *fixDataPage) isEmpty() bool {
	return page.numRows > 0
}

func (page *fixDataPage) toPageData() []byte {
	buf := new(bytes.Buffer)
	if err1 := binary.Write(buf, binary.LittleEndian, page.nextPgNumber); err1 != nil {
		panic("Failed to serialize fix data page")
	}
	if err2 := binary.Write(buf, binary.LittleEndian, page.prevPgNumber); err2 != nil {
		panic("Failed to serialize fix data page")
	}
	if err3 := binary.Write(buf, binary.LittleEndian, page.numRows); err3 != nil {
		panic("Failed to serialize fix data page")
	}
	return utils.PadToPage(append(buf.Bytes(), page.data...))
}

func (page *fixDataPage) searchKey(key interface{}) int {
	fieldId := int(page.meta.ClusterFieldId)
	lo := 0
	hi := int(page.numRows)
	fieldMeta := page.meta.FieldMetas[fieldId]
	for lo < hi {
		mid := (lo + hi) / 2
		rowKey := parseRowField(page.meta, fieldId, page.getRowDataAt(mid))
		if !fieldMeta.cmpField(rowKey, key) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (page *fixDataPage) getRows(key interface{}) [][]interface{} {
	rows := [][]interface{}(nil)
	fieldId := int(page.meta.ClusterFieldId)
	fieldMeta := page.meta.FieldMetas[fieldId]
	for i := page.searchKey(key); i < int(page.numRows); i++ {
		rowKey := parseRowField(page.meta, fieldId, page.getRowDataAt(i))
		if !fieldMeta.cmpField(rowKey, key) && !fieldMeta.cmpField(key, rowKey) {
			rows = append(rows, page.getRowAt(i))
		} else {
			break
		}
	}
	return rows
}

func (page *fixDataPage) canInsert() bool {
	rowSize := page.meta.size()
	headerSize := binary.Size(page.nextPgNumber) + binary.Size(page.prevPgNumber) + binary.Size(page.numRows)
	return headerSize+rowSize*(int(page.numRows)+1) <= int(pager.PGSIZE)
}

func (page *fixDataPage) insertRow(row []interface{}) error {
	if !page.canInsert() {
		return errors.New("Cannot insert since page size limit")
	}
	fieldId := int(page.meta.ClusterFieldId)
	rowSize := page.meta.size()
	i := page.searchKey(row[fieldId])
	page.data = append(page.data[0:i*rowSize],
		append(dumpRow(page.meta, row),
			page.data[i*rowSize:]...)...)
	page.numRows++
	return nil
}

func (page *fixDataPage) deleteRow(key interface{}) {
	fieldId := int(page.meta.ClusterFieldId)
	rowSize := page.meta.size()
	fmeta := page.meta.FieldMetas[fieldId]
	i0 := page.searchKey(key)
	i := i0
	for ; i < int(page.numRows); i++ {
		rowKey := parseRowField(page.meta, fieldId, page.getRowDataAt(i))
		if fmeta.cmpField(rowKey, key) || fmeta.cmpField(key, rowKey) {
			break
		}
	}
	if i == int(page.numRows) {
		page.data = page.data[:i0*rowSize]
		page.numRows = uint32(i0)
	} else if i != i0 {
		page.data = append(page.data[:i0*rowSize], page.data[i*rowSize:]...)
		page.numRows -= uint32((i - i0))
	}
}

func (page *fixDataPage) deleteWithFields(key interface{}, values []FieldValue) {
	fieldId := int(page.meta.ClusterFieldId)
	rowSize := page.meta.size()
	i0 := page.searchKey(key)
	i := i0
	newData := page.data[:i0*rowSize]
	fmeta := page.meta.FieldMetas[fieldId]
	nDelete := 0
	for ; i < int(page.numRows); i++ {
		rowData := page.getRowDataAt(i)
		row := parseRow(page.meta, rowData)
		rowKey := row[fieldId]
		if fmeta.cmpField(rowKey, key) || fmeta.cmpField(key, rowKey) {
			newData = append(newData, page.data[i*rowSize:]...)
			break
		}
		if !page.meta.checkRowSame(row, values) {
			newData = append(newData, rowData...)
		} else {
			nDelete++
		}
	}
	page.numRows -= uint32(nDelete)
	page.data = newData
}

func (page *fixDataPage) getRowAt(i int) []interface{} {
	if i >= int(page.numRows) {
		return nil
	}
	return parseRow(page.meta, page.getRowDataAt(i))
}

func (page *fixDataPage) getRowDataAt(i int) []byte {
	if i >= int(page.numRows) {
		return nil
	}
	rowSize := page.meta.size()
	return page.data[rowSize*i : rowSize*(i+1)]
}

func fixDataPageFromData(pgNumber uint32, meta *RowMeta, data []byte) *fixDataPage {
	buf := bytes.NewBuffer(data)
	var nextPgNumber uint32
	var prevPgNumber uint32
	var numRows uint32
	if err1 := binary.Read(buf, binary.LittleEndian, &nextPgNumber); err1 != nil {
		panic("Failed to deserialize fix data page")
	}
	if err2 := binary.Read(buf, binary.LittleEndian, &prevPgNumber); err2 != nil {
		panic("Failed to deserialize fix data page")
	}
	if err3 := binary.Read(buf, binary.LittleEndian, &numRows); err3 != nil {
		panic("Failed to deserialize fix data page")
	}
	page := &fixDataPage{
		pgNumber:     pgNumber,
		nextPgNumber: nextPgNumber,
		prevPgNumber: prevPgNumber,
		numRows:      numRows,
		meta:         meta,
		data:         data[binary.Size(nextPgNumber)+binary.Size(prevPgNumber)+binary.Size(numRows):],
	}
	page.data = page.data[:page.meta.size()*int(page.numRows)]
	return page
}
