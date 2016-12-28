package view

import (
	"strings"

	core "github.com/gjc13/gsdl/core"
)

func assertTypeCompatible(fmeta core.FieldMeta, v interface{}) bool {
	if v == nil {
		return true
	}
	switch v.(type) {
	case int:
		return fmeta.DataType == core.INT_TYPE
	case float64:
		return fmeta.DataType == core.FLOAT_TYPE
	case float32:
		return fmeta.DataType == core.FLOAT_TYPE
	case string:
		return fmeta.DataType == core.FIX_CHAR_TYPE || fmeta.DataType == core.VAR_CHAR_TYPE
	default:
		return false
	}
}

func columnName2Id(name string, columnNames []string) int {
	for i, n := range columnNames {
		if n == name || strings.HasSuffix(n, "."+name) {
			return i
		}
	}
	return len(columnNames)
}

func ColumnName2Id(name string, columnNames []string) int {
	return columnName2Id(name, columnNames)
}
