package core

import "testing"

var dp_test_meta *RowMeta = &RowMeta{
	FieldMetas: []FieldMeta{
		{INT_TYPE, 4, 1},
		{FLOAT_TYPE, 4, 1},
		{INT_TYPE, 8, 1},
		{FIX_CHAR_TYPE, 200, 1},
	},
	ClusterFieldId: 0,
}
var dp_test_row1 []interface{} = []interface{}{
	int32(100), nil, int64(40), "hello",
}

func TestNullMap(t *testing.T) {
	nullMap := makeNullMap(dp_test_meta, dp_test_row1)
	if dp_test_meta.nullMapSize() != 1 {
		t.Error("Wrong nullmap meta length")
	}
	if len(nullMap) != 1 {
		t.Error("Wrong nullmap length")
	}
	nullMapExpected := []bool{
		false, true, false, false,
	}
	for i := 0; i < 4; i++ {
		if nullMap.getNullMap(i) != nullMapExpected[i] {
			t.Errorf("Wrong nullmap at %d", i)
		}
	}
	nullMap.setNullMap(2)
	if !nullMap.getNullMap(2) {
		t.Errorf("Wrong nullmap at 2")
	}
	nullMap.unSetNullMap(2)
	if nullMap.getNullMap(2) {
		t.Errorf("Wrong nullmap at 2")
	}
}

func TestParseDumpRow(t *testing.T) {
	rowData := dumpRow(dp_test_meta, dp_test_row1)
	if len(rowData) != dp_test_meta.size() {
		t.Error("Wrong dump data size, length %d, data %v", len(rowData), rowData)
	}
	recoverData := parseRow(dp_test_meta, rowData)
	if len(recoverData) != len(dp_test_row1) {
		t.Error("Wrong recovered data length")
	}
	for i := 0; i < len(recoverData); i++ {
		if dp_test_row1[i] != recoverData[i] {
			t.Errorf("Wrong data field at %d, expect %v get %v", i, dp_test_row1[i], recoverData[i])
		}
	}
}
