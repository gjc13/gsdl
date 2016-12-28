package view

import core "github.com/gjc13/gsdl/core"

type RawTableView struct {
	baseView *core.TableView
}

func MakeRawTableView(baseView *core.TableView) *RawTableView {
	if baseView == nil {
		return nil
	}
	return &RawTableView{
		baseView: baseView,
	}
}

func (v *RawTableView) Iter(c chan []interface{}) {
	v.baseView.Reset()
	for {
		row, err := v.baseView.Next()
		if err != nil {
			close(c)
			return
		}
		c <- row
	}
}

func (v *RawTableView) ColumnNames() []string {
	return v.baseView.ColumnNames()
}

func (v *RawTableView) ColumnMetas() []core.FieldMeta {
	return v.baseView.ColumnMetas()
}

func (v *RawTableView) KeyStr(row []interface{}) string {
	return v.baseView.KeyStr(row)
}
