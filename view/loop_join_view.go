package view

import (
	"strings"

	core "github.com/gjc13/gsdl/core"
)

type LoopJoinView struct {
	view1 Viewer
	view2 Viewer
}

func MakeLoopJoinView(view1 Viewer, view2 Viewer) *LoopJoinView {
	if view1 == nil || view2 == nil {
		return nil
	}
	return &LoopJoinView{
		view1: view1,
		view2: view2,
	}
}

func (v *LoopJoinView) Iter(c chan []interface{}) {
	c1 := make(chan []interface{})
	c2 := make(chan []interface{})
	go v.view1.Iter(c1)
	go v.view2.Iter(c2)
	for row1 := range c1 {
		for row2 := range c2 {
			r := make([]interface{}, len(row1)+len(row2))
			copy(r, append(row1, row2...))
			c <- r
		}
	}
	close(c)
}

func (v *LoopJoinView) ColumnNames() []string {
	return append(v.view1.ColumnNames(), v.view2.ColumnNames()...)
}

func (v *LoopJoinView) ColumnMetas() []core.FieldMeta {
	return append(v.view1.ColumnMetas(), v.view2.ColumnMetas()...)
}

func (v *LoopJoinView) KeyStr(row []interface{}) string {
	l := len(v.view1.ColumnNames())
	row1 := row[:l]
	row2 := row[l:]
	return strings.Join([]string{v.view1.KeyStr(row1), v.view2.KeyStr(row2)}, "")
}
