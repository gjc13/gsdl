package core

import (
	"fmt"

	utils "github.com/gjc13/gsdl/utils"
)

const (
	INT_TYPE uint8 = iota
	FLOAT_TYPE
	FIX_CHAR_TYPE
	VAR_CHAR_TYPE
)

type FieldMeta struct {
	DataType   uint8
	FieldWidth uint16
	Nullable   uint8
	Unique     uint8
}

type FieldValue struct {
	FieldId int
	Value   interface{}
}

func (meta *FieldMeta) nullable() bool {
	return meta.Nullable != 0
}

func (meta *FieldMeta) CmpField(lhs interface{}, rhs interface{}) bool {
	return meta.cmpField(lhs, rhs)
}

func (meta *FieldMeta) cmpField(lhs interface{}, rhs interface{}) bool {
	if lhs == nil && rhs == nil {
		return false
	}
	if lhs == nil {
		return true
	}
	if rhs == nil {
		return false
	}
	switch meta.DataType {
	case INT_TYPE:
		l, r := toInt64(lhs), toInt64(rhs)
		return l < r
	case FLOAT_TYPE:
		l, r := toFloat64(lhs), toFloat64(rhs)
		return l < r
	case FIX_CHAR_TYPE:
		return cmpFixChar(meta.FieldWidth, lhs, rhs)
	case VAR_CHAR_TYPE:
		panic("Varchar not supported now")
	default:
		panic("Unkown field type")
	}
	return false
}

func (meta *FieldMeta) isEqual(lhs interface{}, rhs interface{}) bool {
	return !(meta.cmpField(lhs, rhs) || meta.cmpField(rhs, lhs))
}

func (meta *FieldMeta) hash(v interface{}) int64 {
	if v == nil {
		panic("Cannot hash nil")
	}
	switch meta.DataType {
	case INT_TYPE:
		return toInt64(v)
	case FIX_CHAR_TYPE:
		return utils.HashString(v.(string))
	default:
		panic("Field cannot be hashed")
	}

}

func toInt64(v interface{}) int64 {
	switch v := v.(type) {
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return int64(v)
	case int:
		return int64(v)
	default:
		fmt.Printf("%T %v\n", v, v)
		panic("Unkown bit width")
	}
	return 0
}

func ToInt64(v interface{}) int64 {
	return toInt64(v)
}

func toFloat64(v interface{}) float64 {
	switch v := v.(type) {
	case float32:
		return float64(v)
	case float64:
		return float64(v)
	default:
		panic("Unkown bit width")
	}
}

func cmpFixChar(width uint16, lhs interface{}, rhs interface{}) bool {
	return utils.HashString(lhs.(string)) < utils.HashString(rhs.(string))
}
