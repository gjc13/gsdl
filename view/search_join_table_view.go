package view

import (
	"strings"

	core "github.com/gjc13/gsdl/core"
)

type SearchJoinView struct {
	loopView       Viewer
	searchView     *core.TableView
	loopColumnId   int
	searchColumnId int
}

func MakeSearchJoinView(loopView Viewer, searchView *core.TableView, loopColumnName string, searchColumnName string) *SearchJoinView {
	if loopView == nil || searchView == nil {
		return nil
	}
	loopColumnId := columnName2Id(loopColumnName, loopView.ColumnNames())
	searchColumnId := columnName2Id(searchColumnName, searchView.ColumnNames())
	meta1 := loopView.ColumnMetas()
	meta2 := searchView.ColumnMetas()
	if loopColumnId >= len(meta1) || searchColumnId > len(meta2) {
		return nil
	}
	if meta1[loopColumnId].DataType != meta2[searchColumnId].DataType {
		return nil
	}
	return &SearchJoinView{
		loopView:       loopView,
		searchView:     searchView,
		loopColumnId:   loopColumnId,
		searchColumnId: searchColumnId,
	}
}

func (v *SearchJoinView) Iter(c chan []interface{}) {
	c1 := make(chan []interface{})
	go v.loopView.Iter(c1)
	for row1 := range c1 {
		searchKey := row1[v.loopColumnId]
		rows, err := v.searchView.Search(v.searchColumnId, searchKey)
		if err != nil {
			close(c)
			return
		}
		for _, row := range rows {
			r := make([]interface{}, len(row1)+len(row))
			copy(r, append(row1, row...))
			c <- r
		}
	}
	close(c)
}

func (v *SearchJoinView) ColumnNames() []string {
	return append(v.loopView.ColumnNames(), v.searchView.ColumnNames()...)
}

func (v *SearchJoinView) ColumnMetas() []core.FieldMeta {
	return append(v.loopView.ColumnMetas(), v.searchView.ColumnMetas()...)
}

func (v *SearchJoinView) KeyStr(row []interface{}) string {
	l := len(v.loopView.ColumnNames())
	row1 := row[:l]
	row2 := row[l:]
	return strings.Join([]string{v.loopView.KeyStr(row1), v.searchView.KeyStr(row2)}, "")
}
