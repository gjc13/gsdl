package core

import (
	"encoding/binary"
	"math"
)

type nullMap []byte

func makeNullMap(meta *RowMeta, row []interface{}) nullMap {
	nm := nullMap(make([]byte, meta.nullMapSize()))
	for i, v := range row {
		if v == nil {
			nm.setNullMap(i)
		} else {
			nm.unSetNullMap(i)
		}
	}
	return nm
}

func (m nullMap) setNullMap(i int) {
	m[i/8] |= (1 << (uint(i) % 8))
}

func (m nullMap) unSetNullMap(i int) {
	m[i/8] &= (^(1 << (uint(i) % 8)))
}

func (m nullMap) getNullMap(i int) bool {
	return (m[i/8] & (1 << (uint(i) % 8))) > 0
}

func parseRow(meta *RowMeta, data []byte) []interface{} {
	row := []interface{}(nil)
	nm := nullMap(data[0:meta.nullMapSize()])
	offset := meta.nullMapSize()
	for i := 0; i < len(meta.FieldMetas); i++ {
		if nm.getNullMap(i) {
			row = append(row, nil)
		} else {
			row = append(row, parseField(
				meta.FieldMetas[i].DataType,
				meta.FieldMetas[i].FieldWidth,
				data[offset:offset+int(meta.FieldMetas[i].FieldWidth)]))
		}
		offset += int(meta.FieldMetas[i].FieldWidth)
	}
	return row
}

func parseRowField(meta *RowMeta, fieldId int, data []byte) interface{} {
	offset := meta.nullMapSize()
	if meta.FieldMetas[fieldId].nullable() {
		nm := nullMap(data[:offset])
		if nm.getNullMap(fieldId) {
			return nil
		}
	}
	for i := 0; i < fieldId; i++ {
		offset += int(meta.FieldMetas[i].FieldWidth)
	}
	return parseField(meta.FieldMetas[fieldId].DataType, meta.FieldMetas[fieldId].FieldWidth,
		data[offset:offset+int(meta.FieldMetas[fieldId].FieldWidth)])
}

func parseField(fieldType uint8, width uint16, data []byte) interface{} {
	switch fieldType {
	case INT_TYPE:
		return parseInt(width, data)
	case FLOAT_TYPE:
		return parseFloat(width, data)
	case FIX_CHAR_TYPE:
		return parseFixChar(width, data)
	case VAR_CHAR_TYPE:
		panic("Does not support varchar for now")
	default:
		panic("Unknown field type")
	}
	return nil
}

func parseInt(width uint16, data []byte) interface{} {
	switch width {
	case 1:
		return int8(data[0])
	case 2:
		return int16(binary.LittleEndian.Uint16(data))
	case 4:
		return int32(binary.LittleEndian.Uint32(data))
	case 8:
		return int64(binary.LittleEndian.Uint64(data))
	default:
		panic("Not supported bit width for int")
	}
	return nil
}

func parseFloat(width uint16, data []byte) interface{} {
	switch width {
	case 4:
		bits := binary.LittleEndian.Uint32(data)
		return math.Float32frombits(bits)
	case 8:
		bits := binary.LittleEndian.Uint64(data)
		return math.Float64frombits(bits)
	default:
		panic("Not supported bit width for float")
	}
	return nil
}

func parseFixChar(width uint16, data []byte) interface{} {
	for i, v := range data {
		if v == 0 {
			return string(data[0:i])
		}
	}
	return string(data)
}

func dumpRow(meta *RowMeta, row []interface{}) []byte {
	data := []byte(nil)
	for i := 0; i < len(row); i++ {
		data = append(data, dumpField(meta.FieldMetas[i].DataType,
			meta.FieldMetas[i].FieldWidth,
			row[i])...)
	}
	nm := makeNullMap(meta, row)
	data = append(nm, data...)
	return data
}

func dumpField(fieldType uint8, width uint16, field interface{}) []byte {
	if field == nil {
		return make([]byte, width)
	}
	switch fieldType {
	case INT_TYPE:
		return dumpInt(width, field)
	case FLOAT_TYPE:
		return dumpFloat(width, field)
	case FIX_CHAR_TYPE:
		return dumpFixChar(width, field)
	case VAR_CHAR_TYPE:
		panic("Does not support varchar for now")
	default:
		panic("Unknown field type")
	}
	return []byte(nil)
}

func dumpInt(width uint16, field interface{}) []byte {
	switch width {
	case 1:
		return ([]byte{uint8(toInt64(field))})[:]
	case 2:
		data := make([]byte, 2)
		binary.LittleEndian.PutUint16(data, uint16(toInt64(field)))
		return data
	case 4:
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, uint32(toInt64(field)))
		return data
	case 8:
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, uint64(toInt64(field)))
		return data
	default:
		panic("Not supported bit width for int")
	}
	return []byte(nil)
}

func dumpFloat(width uint16, field interface{}) []byte {
	switch width {
	case 4:
		data := make([]byte, 4)
		binary.LittleEndian.PutUint32(data, math.Float32bits(float32(toFloat64(field))))
		return data
	case 8:
		data := make([]byte, 8)
		binary.LittleEndian.PutUint64(data, math.Float64bits(toFloat64(field)))
		return data
	default:
		panic("Not supported bit width for float")
	}
	return []byte(nil)
}

func dumpFixChar(width uint16, field interface{}) []byte {
	str := field.(string)
	data := make([]byte, int(width))
	copy(data, str)
	return data
}
