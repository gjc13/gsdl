package page_map

import (
	"testing"
)

func TestFreeMap(t *testing.T) {
	freeMap := MakeFreePageMap(0, 4096)
	if freeMap.Size() != 4096 {
		t.Errorf("Wrong initial size %d\n", freeMap.Size())
	}
	if freeMap.NumFree() != 4096 {
		t.Errorf("Wrong initial free size %d\n", freeMap.NumFree())
	}
	if freeMap.NextFreePageNumber() != 0 {
		t.Errorf("Wrong initial free page number %d\n", freeMap.NextFreePageNumber())
	}
	freeMap.Set(0)
	if freeMap.NextFreePageNumber() != 1 {
		t.Errorf("Wrong free page number %d\n", freeMap.NextFreePageNumber())
	}
	if freeMap.Size() != 4096 {
		t.Errorf("Wrong free size %d\n", freeMap.NumFree())
	}
	if freeMap.NumFree() != 4095 {
		t.Errorf("Wrong num free %d\n", freeMap.NumFree())
	}
	freeMap.Set(0)
	freeMap.Set(3)
	if freeMap.NextFreePageNumber() != 1 {
		t.Errorf("Wrong free page number %d\n", freeMap.NextFreePageNumber())
	}
	if freeMap.NumFree() != 4094 {
		t.Errorf("Wrong num free %d\n", freeMap.NumFree())
	}
	freeMap.UnSet(0)
	freeMap.UnSet(0)
	freeMap.UnSet(3)
	if freeMap.NextFreePageNumber() != 0 {
		t.Errorf("Wrong free page number %d\n", freeMap.NextFreePageNumber())
	}
	if freeMap.NumFree() != 4096 {
		t.Errorf("Wrong num free %d\n", freeMap.NumFree())
	}
	if freeMap.Size() != 4096 {
		t.Errorf("Wrong num free %d\n", freeMap.NumFree())
	}
}

func TestSerialize(t *testing.T) {
	freeMap := MakeFreePageMap(0, 4096)
	freeMap.Set(0)
	freeMap.Set(3)
	data := freeMap.Serialize()
	newMap := Deserialize(0, 4096, data)
	if newMap.NextFreePageNumber() != 1 {
		t.Errorf("Wrong free page number %d\n", freeMap.NextFreePageNumber())
	}
	if newMap.NumFree() != 4094 {
		t.Errorf("Wrong num free %d\n", freeMap.NumFree())
	}
}
