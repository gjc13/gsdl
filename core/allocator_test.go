package core

import (
	"testing"

	pager "github.com/gjc13/gsdl/pager"
)

func TestAllocPage(t *testing.T) {
	wt := &pager.WriteTransaction{}
	wt.StartTransaction("/tmp/test.gsdl")
	wt.WritePage(0, make([]byte, 4096))
	wt.Sync()
	ctx := &DbContext{
		transaction: wt,
	}
	p1, _ := allocPage(ctx)
	if p1 != 2 {
		t.Error("Wrong allocated first page")
	}
	p2, _ := allocPage(ctx)
	if p2 != 3 {
		t.Error("Wrong allocated second page")
	}
	freePage(ctx, 2)
	p3, _ := allocPage(ctx)
	if p3 != 2 {
		t.Error("Wrong allocte after free")
	}
	wt.EndTransaction()
	wt.StartTransaction("/tmp/test.gsdl")
	ctx = &DbContext{
		transaction: wt,
	}
	p4, _ := allocPage(ctx)
	if p4 != 4 {
		t.Error("Wrong alloc page after recover")
	}
	freePage(ctx, 4)
	freePage(ctx, 3)
	freePage(ctx, 2)
	p5, _ := allocPage(ctx)
	if p5 != 2 {
		t.Error("Wrong final alloc")
	}
	freePage(ctx, 2)
	wt.EndTransaction()
}
