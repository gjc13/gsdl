package view

import (
	core "github.com/gjc13/gsdl/core"
)

type UnionView struct {
	view1 Viewer
	view2 Viewer
}

func MakeUnionView(view1 Viewer, view2 Viewer) *UnionView {
	if view1 == nil || view2 == nil {
		return nil
	}
	names1, names2 := view1.ColumnNames(), view2.ColumnNames()
	if len(names1) != len(names2) {
		return nil
	}
	for i := 0; i < len(names1); i++ {
		if names1[i] != names2[i] {
			return nil
		}
	}
	return &UnionView{
		view1: view1,
		view2: view2,
	}
}

func (v *UnionView) Iter(c chan []interface{}) {
	c1 := make(chan []interface{})
	m := make(map[string][]interface{})
	go v.view1.Iter(c1)
	for row := range c1 {
		m[v.view1.KeyStr(row)] = row
		c <- row
	}
	c2 := make(chan []interface{})
	go v.view2.Iter(c2)
	for row := range c2 {
		if _, ok := m[v.view2.KeyStr(row)]; !ok {
			c <- row
		}
	}
	close(c)
}

func (v *UnionView) ColumnNames() []string {
	return v.view1.ColumnNames()
}

func (v *UnionView) ColumnMetas() []core.FieldMeta {
	return v.view1.ColumnMetas()
}

func (v *UnionView) KeyStr(row []interface{}) string {
	return v.view1.KeyStr(row)
}
