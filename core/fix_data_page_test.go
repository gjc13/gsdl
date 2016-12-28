package core

//import "testing"
//
//var fp_test_meta *RowMeta = &RowMeta{
//	FieldMetas: []FieldMeta{
//		{INT_TYPE, 4, 1},
//		{FLOAT_TYPE, 4, 1},
//		{INT_TYPE, 8, 1},
//		{FIX_CHAR_TYPE, 200, 1},
//	},
//	ClusterFieldId: 0,
//}

//func TestEmptyPage(t *testing.T) {
//	page := &fixDataPage{
//		pgNumber:     0,
//		nextPgNumber: 1,
//		numRows:      0,
//		meta:         fp_test_meta,
//		data:         make([]byte, 0),
//	}
//	if len(page.getRows(1)) != 0 {
//		t.Error("Wrong empty page find")
//	}
//	if page.getRowAt(0) != nil {
//		t.Error("Wrong data at 0")
//	}
//	dump_data := page.toPageData()
//	if len(dump_data) != 4096 {
//		t.Error("Wrong empty page dump size")
//	}
//	recover_page := fixDataPageFromData(0, fp_test_meta, dump_data)
//	if recover_page.pgNumber != 0 {
//		t.Error("Wrong empty page recover page number")
//	}
//	if recover_page.nextPgNumber != 1 {
//		t.Error("Wrong empty page next page number")
//	}
//	if len(recover_page.getRows(1)) != 0 {
//		t.Error("Wrong empty recover_page find")
//	}
//	if recover_page.getRowAt(0) != nil {
//		t.Error("Wrong data at 0")
//	}
//}
//
//func TestPageInsertDelete(t *testing.T) {
//	page := &fixDataPage{
//		pgNumber:     0,
//		nextPgNumber: 1,
//		numRows:      0,
//		meta:         fp_test_meta,
//		data:         make([]byte, 0),
//	}
//	test_row := []interface{}{
//		int32(0), nil, int64(40), "hello",
//	}
//	n := (4096 - 12) / fp_test_meta.size()
//	for i := 0; i < n-1; i++ {
//		test_row[0] = int32(i)
//		page.insertRow(test_row)
//	}
//	page.insertRow(test_row)
//	if int(page.numRows) != n {
//		t.Error("Wrong num rows after insert")
//	}
//	err := page.insertRow(test_row)
//	if err == nil {
//		t.Error("Too much rows inserted!")
//	}
//	expectRowFound(page, int32(-1), 0, t)
//	expectRowFound(page, int32(n+1), 0, t)
//	for i := 0; i < n-2; i++ {
//		expectRowFound(page, int32(i), 1, t)
//	}
//	expectRowFound(page, int32(n-2), 2, t)
//	page.deleteRow(int32(1))
//	expectRowFound(page, int32(1), 0, t)
//	page.deleteRow(int32(n - 2))
//	expectRowFound(page, int32(n-2), 0, t)
//	if int(page.numRows) != n-3 {
//		t.Error("Wrong num rows after delete")
//	}
//	dump_data := page.toPageData()
//	if len(dump_data) != 4096 {
//		t.Error("Wrong empty page dump size")
//	}
//	recover_page := fixDataPageFromData(0, fp_test_meta, dump_data)
//	if recover_page.pgNumber != 0 {
//		t.Error("Wrong page recover page number")
//	}
//	if recover_page.nextPgNumber != 1 {
//		t.Error("Wrong page next page number")
//	}
//	if int(page.numRows) != n-3 {
//		t.Error("Wrong recover page num rows")
//	}
//	test_row[0] = int32(1)
//	page.insertRow(test_row)
//	if int(page.numRows) != n-2 {
//		t.Error("Wrong recover page num rows after insert")
//	}
//	for i := 0; i < n-2; i++ {
//		expectRowFound(page, int32(i), 1, t)
//	}
//	test_row[0] = nil
//	page.insertRow(test_row)
//	page.insertRow(test_row)
//	expectRowFound(page, nil, 2, t)
//	page.deleteRow(nil)
//	expectRowFound(page, nil, 0, t)
//}
//
//func expectRowFound(page *fixDataPage, key interface{}, num int, t *testing.T) {
//	found_rows := page.getRows(key)
//	if len(found_rows) != num {
//		t.Errorf("Row for key %v num error, get %d rows expect %d rows", key, num, len(found_rows))
//	}
//	for i := 0; i < len(found_rows); i++ {
//		if found_rows[i][0] != key {
//			t.Errorf("Wrong row, expect key %v get key %v", key, found_rows[i][0])
//		}
//	}
//}
