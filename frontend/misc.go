package frontend

import (
	"strconv"
	"strings"

	"github.com/gjc13/gsdl/core"
	"github.com/gjc13/gsdl/view"
)

func nameContains(tableNames []string, tableName string) bool {
	for _, n := range tableNames {
		if n == tableName {
			return true
		}
	}
	return false
}

func addTableName(tableName string, columnName string) string {
	if len(strings.Split(columnName, ".")) == 1 {
		return tableName + "." + columnName
	}
	return columnName
}

func divideColumnName(columnName string) (string, string) {
	names := strings.Split(columnName, ".")
	if len(names) != 2 {
		panic("Wrong column name")
	}
	return names[0], names[1]
}

func (e *Engine) isColumnName(op string) bool {
	return !e.isConstant(op)
}

func (e *Engine) isConstant(op string) bool {
	return op == "null" || len(op) == 0 || e.isInteger(op) || e.isVarChar(op)
}

func (e *Engine) isInteger(op string) bool {
	_, err := strconv.Atoi(op)
	return err == nil
}

func (e *Engine) isVarChar(op string) bool {
	return op[0] == '\''
}

func (e *Engine) isTableNamesLegal(tableNames []string) bool {
	legalNames := e.ctx.GetTableNames()
	for _, n := range tableNames {
		if !nameContains(legalNames, n) {
			return false
		}
	}
	return true
}

func (e *Engine) isCompatible(lhs string, rhs string) bool {
	if !e.isColumnName(lhs) && !e.isColumnName(rhs) {
		return false
	}
	if e.isColumnName(lhs) && e.isColumnName(rhs) {
		lTableName, lColumnName := divideColumnName(lhs)
		rTableName, rColumnName := divideColumnName(rhs)
		if lTableName == rTableName {
			return false
		}
		metal, errl := e.getFieldMeta(lTableName, lColumnName)
		if errl != nil {
			return false
		}
		metar, errr := e.getFieldMeta(rTableName, rColumnName)
		if errr != nil {
			return false
		}
		return metal.DataType == metar.DataType
	}
	if e.isColumnName(rhs) {
		lhs, rhs = rhs, lhs
	}
	return e.isValueTypeCompatible(lhs, rhs)
}

func (e *Engine) isColumnNameLegal(colName string, v view.Viewer) bool {
	return view.ColumnName2Id(colName, v.ColumnNames()) < len(v.ColumnNames())
}

func (e *Engine) isReduceColumnLegal(colName string, v view.Viewer) bool {
	idx := view.ColumnName2Id(colName, v.ColumnNames())
	if idx >= len(v.ColumnNames()) {
		return false
	}
	fmeta := v.ColumnMetas()[idx]
	return fmeta.DataType == core.INT_TYPE
}

func (e *Engine) isValueTypeCompatible(columnName string, value string) bool {
	tableName, fieldName := divideColumnName(columnName)
	meta, err := e.getFieldMeta(tableName, fieldName)
	if err != nil {
		return false
	}
	if meta.Nullable != 0 && (len(value) == 0 || value == "null") {
		return true
	}
	switch meta.DataType {
	case core.INT_TYPE:
		return e.isInteger(value)
	case core.FIX_CHAR_TYPE:
		fallthrough
	case core.VAR_CHAR_TYPE:
		return e.isVarChar(value)
	default:
		return false
	}
}

func (e *Engine) toCompatibleValue(columnName string, value string) interface{} {
	tableName, fieldName := divideColumnName(columnName)
	meta, err := e.getFieldMeta(tableName, fieldName)
	if err != nil || value == "null" {
		return nil
	}
	if len(value) == 0 {
		return nil
	}
	switch meta.DataType {
	case core.INT_TYPE:
		v, _ := strconv.Atoi(value)
		return v
	case core.FIX_CHAR_TYPE:
		fallthrough
	case core.VAR_CHAR_TYPE:
		return value[1 : len(value)-1]
	default:
		return nil
	}
}

func (e *Engine) getFieldMeta(tableName string, fieldName string) (core.FieldMeta, error) {
	v, err := e.ctx.CreateTableView(tableName)
	if err != nil {
		return core.FieldMeta{}, err
	}
	i := view.ColumnName2Id(fieldName, v.ColumnNames())
	if i < len(v.ColumnNames()) {
		return v.ColumnMetas()[i], nil
	}
	return core.FieldMeta{}, ERR_NOCOLUMN
}

func (e *Engine) getFieldMetas(tableName string) ([]core.FieldMeta, error) {
	v, err := e.ctx.CreateTableView(tableName)
	if err != nil {
		return nil, err
	}
	return v.ColumnMetas(), nil
}

func (e *Engine) getFieldNames(tableName string) ([]string, error) {
	v, err := e.ctx.CreateTableView(tableName)
	if err != nil {
		return nil, err
	}
	return v.ColumnNames(), nil
}
