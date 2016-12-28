package main

import core "github.com/gjc13/gsdl/core"

var db_test_meta1 *core.RowMeta = &core.RowMeta{
	FieldMetas: []core.FieldMeta{
		{core.INT_TYPE, 4, 0, 1},
		{core.INT_TYPE, 8, 1, 0},
		{core.FIX_CHAR_TYPE, 64, 1, 0},
	},
	ClusterFieldId: 0,
}

var db_column_names1 []string = []string{
	"book_id", "book_price", "book_name",
}

var db_test_meta2 *core.RowMeta = &core.RowMeta{
	FieldMetas: []core.FieldMeta{
		{core.INT_TYPE, 4, 0, 1},
		{core.INT_TYPE, 4, 1, 0},
		{core.INT_TYPE, 8, 1, 0},
	},
	ClusterFieldId: 0,
}

var db_column_names2 []string = []string{
	"customer_id", "book_id", "price",
}

func main() {
	ctx, _ := core.StartUseDatabase("orderDB")
	view, _ := ctx.CreateTableView("publisher")
	view.Print()
	//rows, _ := view.Search(0, 104946)
	//fmt.Println(len(rows))
	ctx.EndUseDatabase()
	//if err := core.CreateDatabase("test_db2"); err != nil {
	//	fmt.Printf("%v\n", err)
	//}
	//ctx, err1 := core.StartUseDatabase("test_db2")
	//if err1 != nil {
	//	fmt.Println("Cannot Use database")
	//}
	//if err := ctx.CreateTable("books", db_column_names1, db_test_meta1); err != nil {
	//	fmt.Printf("Cannot create books table %v\n", err.Error())
	//}
	//if err := ctx.CreateTable("orders", db_column_names2, db_test_meta2); err != nil {
	//	fmt.Println("Cannot create orders table")
	//}
	//view, _ := ctx.CreateTableView("books")
	//view.Insert([]interface{}{
	//	0, 20, "book1",
	//})
	//view.Delete(0, nil)
	//for i := 0; i < 20000; i++ {
	//	view.Insert([]interface{}{
	//		i, 20, "book1",
	//	})
	//}
	//for i := 20000; i < 40000; i++ {
	//	view.Insert([]interface{}{
	//		i, 40, "book2",
	//	})
	//}
	//for i := 40000; i < 60000; i++ {
	//	view.Insert([]interface{}{
	//		i, nil, "book1",
	//	})
	//}
	////for i := 0; i < 60000; i++ {
	////	if i%100 == 0 {
	////		fmt.Println(i)
	////	}
	////	view.Insert([]interface{}{
	////		i, i, fmt.Sprintf("%v", i),
	////	})
	////}

	//fmt.Println("Insert done")
	////fmt.Println()
	////res1, _ := view.Search(1, 20)
	////fmt.Println(len(res1))
	////view.Delete(50000, nil)
	////res2, _ := view.Search(1, nil)
	////fmt.Println(len(res2))
	//view.Delete(0, nil)
	////view.Print()
	//fmt.Println("Searching...")
	//res3, _ := view.Search(2, "book1")
	//fmt.Println(len(res3))
	//ctx.EndUseDatabase()
}
