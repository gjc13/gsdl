package main

import (
	"fmt"

	core "github.com/gjc13/gsdl/core"
	view "github.com/gjc13/gsdl/view"
)

var db_test_meta1 *core.RowMeta = &core.RowMeta{
	FieldMetas: []core.FieldMeta{
		{core.INT_TYPE, 4, 0},
		{core.INT_TYPE, 8, 1},
		{core.FIX_CHAR_TYPE, 64, 1},
	},
	ClusterFieldId: 0,
}

var db_column_names1 []string = []string{
	"book_id", "book_price", "book_name",
}

var db_test_meta2 *core.RowMeta = &core.RowMeta{
	FieldMetas: []core.FieldMeta{
		{core.INT_TYPE, 4, 0},
		{core.INT_TYPE, 4, 1},
		{core.FIX_CHAR_TYPE, 64, 1},
	},
	ClusterFieldId: 0,
}

var db_column_names2 []string = []string{
	"order_id", "book_id", "customer_name",
}

func PrintView(v view.Viewer) {
	fmt.Println(v.ColumnNames())
	c := make(chan []interface{})
	go v.Iter(c)
	for row := range c {
		fmt.Println(row)
	}
}

func main() {
	if err := core.CreateDatabase("test_db2"); err != nil {
		fmt.Printf("%v\n", err)
	}
	ctx, err1 := core.StartUseDatabase("test_db2")
	if err1 != nil {
		fmt.Println("Cannot Use database")
	}
	if err := ctx.CreateTable("books", db_column_names1, db_test_meta1); err != nil {
		fmt.Printf("Cannot create books table %v\n", err.Error())
	}
	if err := ctx.CreateTable("orders", db_column_names2, db_test_meta2); err != nil {
		fmt.Println("Cannot create orders table")
	}
	viewBooks, _ := ctx.CreateTableView("books")
	for i := 0; i < 20; i++ {
		viewBooks.Insert([]interface{}{
			i, 20, "book1",
		})
	}
	for i := 40; i < 60; i++ {
		viewBooks.Insert([]interface{}{
			i, nil, "book1",
		})
	}
	for i := 20; i < 40; i++ {
		viewBooks.Insert([]interface{}{
			i, 40, "book2",
		})
	}
	for i := 60; i < 80; i++ {
		viewBooks.Insert([]interface{}{
			i, 10, "book2",
		})
	}
	viewOrders, _ := ctx.CreateTableView("orders")
	for i := 0; i < 20; i++ {
		viewOrders.Insert([]interface{}{
			i * 3, 0, fmt.Sprintf("customer%d", i),
		})
		viewOrders.Insert([]interface{}{
			i*3 + 1, 25, fmt.Sprintf("customer%d", i),
		})
		viewOrders.Insert([]interface{}{
			i*3 + 2, 40, fmt.Sprintf("customer%d", i),
		})

	}
	vb := view.MakeRawTableView(viewBooks)
	PrintView(vb)
	vo := view.MakeRawTableView(viewOrders)
	PrintView(vo)
	v2 := view.MakeEmptyView(vb)
	PrintView(v2)
	v3 := view.MakeSearchRawTableView(viewOrders, "customer_name", "customer0")
	PrintView(v3)
	v4 := view.MakeSearchJoinView(vb, viewOrders, "books.book_id", "orders.book_id")
	PrintView(v4)
	v5 := view.MakeFilterView(v4, "books.book_price", view.COND_L, 25)
	PrintView(v5)
	v6 := view.MakeFilterView(v4, "books.book_price", view.COND_EQ, nil)
	PrintView(v6)
	v7 := view.MakeFilterView(v4, "orders.customer_name", view.COND_EQ, "customer7")
	PrintView(v7)
	PrintView(view.MakeIntersectView(v6, v7))
	PrintView(view.MakeUnionView(v6, v7))
	ctx.EndUseDatabase()
}
