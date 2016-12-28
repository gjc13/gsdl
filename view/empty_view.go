package view

import core "github.com/gjc13/gsdl/core"

type EmptyView struct {
	baseView Viewer
}

func MakeEmptyView(baseView Viewer) *EmptyView {
	if baseView == nil {
		return nil
	}
	return &EmptyView{
		baseView: baseView,
	}
}

func (v *EmptyView) Iter(c chan []interface{}) {
	close(c)
}

func (v *EmptyView) ColumnNames() []string {
	return v.baseView.ColumnNames()
}

func (v *EmptyView) ColumnMetas() []core.FieldMeta {
	return v.baseView.ColumnMetas()
}

func (v *EmptyView) KeyStr(row []interface{}) string {
	return v.baseView.KeyStr(row)
}
