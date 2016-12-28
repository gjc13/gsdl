package view

import (
	"fmt"

	core "github.com/gjc13/gsdl/core"
)

type Viewer interface {
	Iter(c chan []interface{})
	ColumnNames() []string
	ColumnMetas() []core.FieldMeta
	KeyStr(row []interface{}) string
}

func PrintView(v Viewer) {
	if v == nil {
		return
	}
	fmt.Println(v.ColumnNames())
	c := make(chan []interface{})
	go v.Iter(c)
	for row := range c {
		for _, v := range row {
			fmt.Printf("%v, ", v)
		}
		fmt.Println()
	}
}
