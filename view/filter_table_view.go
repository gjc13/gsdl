package view

import core "github.com/gjc13/gsdl/core"

const (
	COND_L          int = iota
	COND_LE             = iota
	COND_G              = iota
	COND_GE             = iota
	COND_EQ             = iota
	COND_NE             = iota
	COND_LIKE           = iota
	COND_IS_NULL        = iota
	COND_ISNOT_NULL     = iota
)

func cmp(meta *core.FieldMeta, lhs interface{}, rhs interface{}, op int) bool {
	if op != COND_IS_NULL && op != COND_ISNOT_NULL && (rhs == nil || lhs == nil) {
		return false
	}
	switch op {
	case COND_L:
		return meta.CmpField(lhs, rhs)
	case COND_LE:
		return !meta.CmpField(rhs, lhs)
	case COND_G:
		return meta.CmpField(rhs, lhs)
	case COND_GE:
		return !meta.CmpField(lhs, rhs)
	case COND_EQ:
		return !meta.CmpField(lhs, rhs) && !meta.CmpField(rhs, lhs)
	case COND_NE:
		return meta.CmpField(lhs, rhs) || meta.CmpField(rhs, lhs)
	case COND_IS_NULL:
		return lhs == nil
	case COND_ISNOT_NULL:
		return lhs != nil
	default:
		panic("Not implemented")
	}
	return false
}

type FilterView struct {
	baseView       Viewer
	filterColumnId int
	filterOp       int
	rhs            interface{}
}

func MakeFilterView(baseView Viewer, filterColumnName string, filterOp int, rhs interface{}) *FilterView {
	if baseView == nil {
		return nil
	}
	filterColumnId := columnName2Id(filterColumnName, baseView.ColumnNames())
	metas := baseView.ColumnMetas()
	if filterColumnId >= len(metas) {
		return nil
	}
	if !assertTypeCompatible(metas[filterColumnId], rhs) {
		return nil
	}
	return &FilterView{
		baseView:       baseView,
		filterColumnId: filterColumnId,
		filterOp:       filterOp,
		rhs:            rhs,
	}
}

func (v *FilterView) Iter(c chan []interface{}) {
	c1 := make(chan []interface{})
	meta := v.baseView.ColumnMetas()[v.filterColumnId]
	go v.baseView.Iter(c1)
	for row := range c1 {
		if cmp(&meta, row[v.filterColumnId], v.rhs, v.filterOp) {
			c <- row
		}
	}
	close(c)
}

func (v *FilterView) ColumnNames() []string {
	return v.baseView.ColumnNames()
}

func (v *FilterView) ColumnMetas() []core.FieldMeta {
	return v.baseView.ColumnMetas()
}

func (v *FilterView) KeyStr(row []interface{}) string {
	return v.baseView.KeyStr(row)
}
