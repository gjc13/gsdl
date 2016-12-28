package pager

import "testing"

func testWritePage(t *testing.T, wt *WriteTransaction, pgNumber uint32, value byte) {
	data := make([]byte, PGSIZE)
	for i := uint32(0); i < PGSIZE; i++ {
		data[i] = value
	}
	err := wt.WritePage(pgNumber, data)
	if err != nil {
		t.Error("Error cannot wirte to gsdl file")
	}
}

func testReadPage(t *testing.T, transaction TransactionReader, pgNumber uint32, value byte) {
	data, err := transaction.ReadPage(pgNumber)
	if err != nil {
		t.Log(err)
		t.Error("Error in reading page 0, cannot read")
	}
	if len(data) != int(PGSIZE) {
		t.Error("Error in reading page 0, wrong page size")
	}
}

func TestReadTransaction(t *testing.T) {
	TestWriteTransaction(t)
	readTransaction := &ReadTransaction{}
	readTransaction.StartTransaction("/tmp/this_exists.gsdl")
	testReadPage(t, readTransaction, 0, 0)
	readTransaction.EndTransaction()
	readTransaction.StartTransaction("/tmp/this_does_not_exist.gsdl")
	_, err := readTransaction.ReadPage(0)
	if err == nil {
		t.Errorf("Error %v\n", err)
		t.Error("Error not reported for non exist file")
	}
	readTransaction.EndTransaction()
}

func TestWriteTransaction(t *testing.T) {
	writeTransaction := &WriteTransaction{}
	writeTransaction.StartTransaction("/tmp/this_exists.gsdl")
	data := make([]byte, PGSIZE)
	writeTransaction.WritePage(0, data)
	err := writeTransaction.EndTransaction()
	if err != nil {
		t.Error("Error cannot wirte to gsdl file")
	}
}

func TestReadWriteTransaction(t *testing.T) {
	wt := &WriteTransaction{}
	wt.StartTransaction("/tmp/test_transaction.gsdl")
	for i := 0; i < 1024; i++ {
		testWritePage(t, wt, uint32(i), byte(i))
	}
	for i := 0; i < 1024; i++ {
		testReadPage(t, wt, uint32(i), byte(i))
	}
	err := wt.EndTransaction()
	if err != nil {
		t.Error("Error cannot wirte to gsdl file")
	}
	rt := &ReadTransaction{}
	rt.StartTransaction("/tmp/test_transaction.gsdl")
	for i := 0; i < 1024; i++ {
		testReadPage(t, rt, uint32(i), byte(i))
	}
	err2 := rt.EndTransaction()
	if err2 != nil {
		t.Error("Error cannot end read transaction")
	}

}
