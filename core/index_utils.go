package core

import (
	"fmt"
	"sort"
)

type Key int64
type Elem struct {
	Key      Key
	PgNumber uint32
}
type Elems []Elem

func (elems Elems) Len() int { return len(elems) }
func (elems Elems) Less(i, j int) bool {
	return elems[i].Key < elems[j].Key
}

func (elems Elems) Swap(i, j int) { elems[i], elems[j] = elems[j], elems[i] }

func (elems Elems) find(key Key) (idx int, isEqual bool) {
	idx = sort.Search(len(elems), func(i int) bool {
		isEqual = isEqual || elems[i].Key == key
		return elems[i].Key >= key
	})
	return
}

func (elems Elems) insert(elem Elem, maxDegree int, allowOverlap bool) (Elems, error) {
	idx, equal := elems.find(elem.Key)

	if idx >= len(elems) {
		elems = append(elems, elem)
		return elems, nil
	}

	if equal && !allowOverlap {
		return nil, ERR_OVERLAPPED
	}

	newElems := make(Elems, len(elems)+1, maxDegree+1)

	copy(newElems, elems[:idx])
	newElems[idx] = elem
	copy(newElems[idx+1:], elems[idx:])

	return newElems, nil
}

func (elems Elems) delete(key Key, maxDegree int) (Elems, bool) {
	idx, equal := elems.find(key)

	if equal {
		// found
		newElems := make(Elems, len(elems)-1, maxDegree+1)

		copy(newElems, elems[:idx])
		copy(newElems[idx:], elems[idx+1:])

		return newElems, true
	}

	return elems, false
}

func (elems Elems) String() string {
	var elemsStr []string

	for _, _elem := range elems {
		elemsStr = append(elemsStr, fmt.Sprintf("%v: %v", _elem.Key, _elem.PgNumber))
	}

	return fmt.Sprintf("%v", elemsStr)
}
