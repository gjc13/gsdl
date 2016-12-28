package view

import core "github.com/gjc13/gsdl/core"

type SearchRawTableView struct {
	baseView       *core.TableView
	searchColumnId int
	val            interface{}
}

func MakeSearchRawTableView(baseView *core.TableView, searchColumnName string, val interface{}) *SearchRawTableView {
	if baseView == nil {
		return nil
	}
	searchColumnId := columnName2Id(searchColumnName, baseView.ColumnNames())
	metas := baseView.ColumnMetas()
	if searchColumnId >= len(metas) {
		return nil
	}
	if !assertTypeCompatible(metas[searchColumnId], val) {
		return nil
	}
	return &SearchRawTableView{
		baseView:       baseView,
		searchColumnId: searchColumnId,
		val:            val,
	}
}

func (v *SearchRawTableView) Iter(c chan []interface{}) {
	v.baseView.Reset()
	rows, err := v.baseView.Search(v.searchColumnId, v.val)
	if err != nil {
		close(c)
		return
	} else {
		for _, row := range rows {
			c <- row
		}
	}
	close(c)
}

func (v *SearchRawTableView) ColumnNames() []string {
	return v.baseView.ColumnNames()
}

func (v *SearchRawTableView) ColumnMetas() []core.FieldMeta {
	return v.baseView.ColumnMetas()
}

func (v *SearchRawTableView) KeyStr(row []interface{}) string {
	return v.baseView.KeyStr(row)
}
