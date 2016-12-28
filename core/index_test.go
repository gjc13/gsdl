package core

import (
	"testing"

	pager "github.com/gjc13/gsdl/pager"
)

var index_test_wt *pager.WriteTransaction = &pager.WriteTransaction{}

var index_test_ctx *DbContext = &DbContext{
	transaction: index_test_wt,
}

func TestNewTree(t *testing.T) {
	index_test_wt.StartTransaction("/tmp/index_test_1.gsdl")
	index_test_wt.WritePage(0, make([]byte, 4096))
	index_test_wt.Sync()
	tree, _ := createTree(index_test_ctx)
	tree.Insert(Elem{0, 0})
	if tree.RootPgNumber() != 2 {
		t.Errorf("Wrong root page number %d", tree.RootPgNumber())
	}
	index_test_wt.EndTransaction()
}

func TestTreeInsertFind(t *testing.T) {
	index_test_wt.StartTransaction("/tmp/index_test_2.gsdl")
	index_test_wt.WritePage(0, make([]byte, 4096))
	index_test_wt.Sync()
	tree, _ := createTree(index_test_ctx)
	tree.Insert(Elem{0, 1})
	tree.Insert(Elem{20, 5})
	expectFound(tree, 0, 1, t)
	expectFound(tree, 10, 1, t)
	expectNotFound(tree, -10, t)
	expectFound(tree, 25, 5, t)
	err5 := tree.Insert(Elem{0, 3})
	if err5 != ERR_OVERLAPPED {
		t.Errorf("Overlap insert\n")
	}
	index_test_wt.EndTransaction()
}

func TestTreeInsertDeleteFind(t *testing.T) {
	index_test_wt.StartTransaction("/tmp/index_test_3.gsdl")
	index_test_wt.WritePage(0, make([]byte, 4096))
	index_test_wt.Sync()
	tree, _ := createTree(index_test_ctx)
	tree.Insert(Elem{0, 1})
	tree.Insert(Elem{20, 5})
	expectFound(tree, 0, 1, t)
	expectFound(tree, 10, 1, t)
	expectNotFound(tree, -10, t)
	expectFound(tree, 25, 5, t)
	tree.Insert(Elem{30, 10})
	tree.Remove(-1)
	expectFound(tree, 0, 1, t)
	expectFound(tree, 10, 1, t)
	expectNotFound(tree, -10, t)
	expectFound(tree, 25, 5, t)
	expectFound(tree, 30, 10, t)
	tree.Remove(25)
	expectFound(tree, 0, 1, t)
	expectFound(tree, 10, 1, t)
	expectNotFound(tree, -10, t)
	expectFound(tree, 25, 5, t)
	expectFound(tree, 30, 10, t)
	tree.Remove(20)
	expectFound(tree, 0, 1, t)
	expectFound(tree, 10, 1, t)
	expectNotFound(tree, -10, t)
	expectFound(tree, 25, 1, t)
	expectFound(tree, 30, 10, t)
	tree.Remove(0)
	expectNotFound(tree, 0, t)
	expectNotFound(tree, -10, t)
	expectNotFound(tree, 25, t)
	expectFound(tree, 30, 10, t)
	index_test_wt.EndTransaction()
}

func TestTreeBalance(t *testing.T) {
	index_test_wt.StartTransaction("/tmp/index_test_4.gsdl")
	index_test_wt.WritePage(0, make([]byte, 4096))
	index_test_wt.Sync()
	tree, _ := createTree(index_test_ctx)
	for i := 0; i < maxDegree+2; i++ {
		tree.Insert(Elem{Key(i * 10), uint32(i + 2)})
	}
	for i := 0; i < maxDegree+2; i++ {
		expectFound(tree, Key(i*10), uint32(i+2), t)
		expectFound(tree, Key(i*10+1), uint32(i+2), t)
	}
	index_test_wt.EndTransaction()
}

func TestTreeLMerge(t *testing.T) {
	index_test_wt.StartTransaction("/tmp/index_test_5.gsdl")
	index_test_wt.WritePage(0, make([]byte, 4096))
	index_test_wt.Sync()
	tree, _ := createTree(index_test_ctx)
	for i := 0; i < maxDegree+3; i++ {
		tree.Insert(Elem{Key(i * 10), uint32(i + 2)})
	}
	for i := 0; i < 10; i++ {
		tree.Remove(Key(i * 10))
	}
	for i := 10; i < maxDegree+3; i++ {
		expectFound(tree, Key(i*10), uint32(i+2), t)
		expectFound(tree, Key(i*10+1), uint32(i+2), t)
	}
	index_test_wt.EndTransaction()
}

func TestTreeRMerge(t *testing.T) {
	index_test_wt.StartTransaction("/tmp/index_test_7.gsdl")
	index_test_wt.WritePage(0, make([]byte, 4096))
	index_test_wt.Sync()
	tree, _ := createTree(index_test_ctx)
	for i := maxDegree + 2; i >= 0; i-- {
		tree.Insert(Elem{Key(i * 10), uint32(i + 2)})
	}
	for i := 0; i < 10; i++ {
		tree.Remove(Key((maxDegree + 2 - i) * 10))
	}
	for i := 0; i < 1; i++ {
		expectFound(tree, Key(i*10), uint32(i+2), t)
		expectFound(tree, Key(i*10+1), uint32(i+2), t)
	}
	index_test_wt.EndTransaction()
}

func expectFound(tree *Bptree, k Key, pgNumber uint32, t *testing.T) {
	pg, err := tree.Search(k)
	if pg != pgNumber || err != nil {
		t.Errorf("Wrong page found for key %d, get %d err %v\n", k, pg, err)
	}
}

func expectNotFound(tree *Bptree, k Key, t *testing.T) {
	pg, err := tree.Search(k)
	if err != ERR_NOT_FOUND {
		t.Errorf("Wrong page found for key %d, get %d err %v\n", k, pg, err)
	}
}
