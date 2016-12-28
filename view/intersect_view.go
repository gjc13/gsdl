package view

import (
	core "github.com/gjc13/gsdl/core"
)

type IntersectView struct {
	view1 Viewer
	view2 Viewer
}

func MakeIntersectView(view1 Viewer, view2 Viewer) *IntersectView {
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
	return &IntersectView{
		view1: view1,
		view2: view2,
	}
}

func (v *IntersectView) Iter(c chan []interface{}) {
	c1 := make(chan []interface{})
	m := make(map[string][]interface{})
	go v.view1.Iter(c1)
	for row := range c1 {
		m[v.view1.KeyStr(row)] = row
	}
	c2 := make(chan []interface{})
	go v.view2.Iter(c2)
	for row := range c2 {
		if _, ok := m[v.view2.KeyStr(row)]; ok {
			c <- row
		}
	}
	close(c)
}

func (v *IntersectView) ColumnNames() []string {
	return v.view1.ColumnNames()
}

func (v *IntersectView) ColumnMetas() []core.FieldMeta {
	return v.view1.ColumnMetas()
}

func (v *IntersectView) KeyStr(row []interface{}) string {
	return v.view1.KeyStr(row)
}
