package core

import (
	"fmt"
	"testing"
)

var db_test_meta1 *RowMeta = &RowMeta{
	FieldMetas: []FieldMeta{
		{INT_TYPE, 4, 0},
		{INT_TYPE, 8, 1},
		{FIX_CHAR_TYPE, 64, 1},
	},
	ClusterFieldId: 0,
}

var db_column_names1 []string = []string{
	"book_id", "book_price", "book_name",
}

var db_test_meta2 *RowMeta = &RowMeta{
	FieldMetas: []FieldMeta{
		{INT_TYPE, 4, 0},
		{INT_TYPE, 4, 1},
		{INT_TYPE, 8, 1},
	},
	ClusterFieldId: 0,
}

var db_column_names2 []string = []string{
	"customer_id", "book_id", "price",
}

func TestTableInsertDelete(t *testing.T) {
	if err := CreateDatabase("test_db1"); err != nil {
		t.Errorf("%v", err)
	}
	ctx, err1 := StartUseDatabase("test_db1")
	if err1 != nil {
		t.Error("Cannot Use database")
	}
	if err := ctx.CreateTable("books", db_column_names1, db_test_meta1); err != nil {
		t.Errorf("Cannot create books table %v", err.Error())
	}
	if err := ctx.CreateTable("books", db_column_names1, db_test_meta1); err != ERR_OVERLAPPED {
		t.Error("Books table overlap")
	}
	if err := ctx.CreateTable("orders", db_column_names2, db_test_meta2); err != nil {
		t.Error("Cannot create orders table")
	}
	if err := ctx.CreateTable("orders", db_column_names2, db_test_meta2); err != ERR_OVERLAPPED {
		t.Error("Orders table overlap")
	}
	ctx.EndUseDatabase()
}

func TestTableView(t *testing.T) {
	ctx, _ := StartUseDatabase("test_db1")
	view, err1 := ctx.CreateTableView("books")
	if err1 != nil {
		t.Error("Cannot create view")
	}
	if hasNext, err := view.HasNext(); hasNext || err != nil {
		t.Errorf("Wrong hasnext %v %v", hasNext, err)
	}
	view.Insert([]interface{}{
		0, 20, "book1",
	})
	res1, _ := view.Search(0, 0)
	if len(res1) != 1 || res1[0][1].(int64) != 20 {
		fmt.Printf("%v\n", res1)
		t.Error("Wrong found")
	}
	view.Delete(0, nil)
	if hasNext, err := view.HasNext(); hasNext || err != nil {
		t.Errorf("Wrong hasnext %v %v", hasNext, err)
	}
	for i := 0; i < 5; i++ {
		view.Insert([]interface{}{
			i, 20, fmt.Sprintf("book%d", i),
		})
	}
}
