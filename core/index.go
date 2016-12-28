package core

import (
	"sort"

	"github.com/gjc13/gsdl/pager"
)

const maxDegree = 320

type Bptree struct {
	ctx          *DbContext
	rootPgNumber uint32
}

func (tree *Bptree) RootPgNumber() uint32 {
	return tree.rootPgNumber
}

func (tree *Bptree) loadIndexPage(pgNumber uint32) (*indexPage, error) {
	rt := tree.ctx.transaction.(pager.TransactionReader)
	data, err := rt.ReadPage(pgNumber)
	if err != nil {
		rt.AbortTransaction()
		return nil, err
	}
	return indexPageFromData(pgNumber, data), nil
}

func (tree *Bptree) saveIndexPage(page *indexPage) error {
	wt, ok := tree.ctx.transaction.(*pager.WriteTransaction)
	if !ok {
		panic("not write transaction when saving")
	}
	return wt.WritePage(page.PgNumber, page.toPageData())
}

func createTree(ctx *DbContext) (*Bptree, error) {
	pgNumber, err := allocPage(ctx)
	if err != nil {
		return nil, err
	}
	rnode := &indexPage{
		PgNumber:     pgNumber,
		Children:     make([]Elem, 0),
		Internal:     0,
		NextPgNumber: 0,
		PrevPgNumber: 0,
	}
	tree := &Bptree{
		ctx:          ctx,
		rootPgNumber: pgNumber,
	}
	if err1 := tree.saveIndexPage(rnode); err1 != nil {
		return nil, err
	}
	return tree, nil
}

func (tree *Bptree) Insert(elem Elem) error {
	// create root node if it is not exist
	if tree.rootPgNumber == 0 {
		pgNumber, err := allocPage(tree.ctx)
		if err != nil {
			return err
		}
		rnode := &indexPage{
			PgNumber:     pgNumber,
			Children:     make([]Elem, 0),
			Internal:     0,
			NextPgNumber: 0,
			PrevPgNumber: 0,
		}
		rnode.Children = append(rnode.Children, elem)
		err = tree.saveIndexPage(rnode)
		if err != nil {
			return err
		}
		tree.rootPgNumber = rnode.PgNumber
		return nil
	}
	// find paths pass by
	paths, err := tree.findToInsert(elem.Key)

	// insert element into last index node
	lastPath := paths[len(paths)-1]

	err = lastPath.insertElem(elem, maxDegree, false)
	if err != nil {
		return err
	}
	err = tree.saveIndexPage(lastPath)
	if err != nil {
		return err
	}

	// update parent key
	if len(paths) > 1 {
		err = tree.updateParent(lastPath, paths[len(paths)-2])
		if err != nil {
			return err
		}
	}

	// do balancing if index node has children more than maxDegree
	for i := len(paths) - 1; i >= 0; i-- {
		path := paths[i]

		if len(path.Children) > maxDegree {
			err = tree.balance(paths[:i+1])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tree *Bptree) Remove(key Key) error {
	// find paths
	paths, err := tree.findToExactElem(key)
	if err != nil {
		return err
	}

	lenPaths := len(paths)

	// if only root
	if lenPaths == 1 {
		root := paths[0]
		root.deleteElem(key, maxDegree)
		return tree.saveIndexPage(root)
	}

	allowedDegree := maxDegree / 2
	var curr *indexPage

	// do balancing if index node has children less than tree.maxDegree / 2
	for i := lenPaths - 1; i >= 0; i-- {
		curr = paths[i]

		if i == 0 { // at root
			if len(curr.Children) <= 1 {
				if len(paths) > 1 {
					child, err2 := tree.loadIndexPage(curr.Children[0].PgNumber)
					if err2 != nil {
						return err2
					}
					curr.Children = child.Children
					curr.Internal = child.Internal
					curr.PrevPgNumber = child.PrevPgNumber
					curr.NextPgNumber = child.NextPgNumber
					if err2 = tree.saveIndexPage(curr); err2 != nil {
						return err2
					}
					return freePage(tree.ctx, paths[1].PgNumber)
				} else { //empty root
					freePage(tree.ctx, tree.rootPgNumber)
					tree.rootPgNumber = 0
				}
			}
			return nil
		}

		if i == lenPaths-1 { // at first loop (last node in paths)
			// delete the element at belong node
			curr.deleteElem(key, maxDegree)
			err = tree.saveIndexPage(curr)
			if err != nil {
				return nil
			}
		}

		if len(curr.Children) < allowedDegree {
			ok, errRedis := tree.redistribution(paths[:i+1], allowedDegree)
			if errRedis != nil {
				return errRedis
			}
			if !ok {
				err = tree.merge(paths[:i+1])
				if err != nil {
					return err
				}
			}
		} else {
			//update parent key
			parent := paths[i-1]
			err = tree.updateParent(curr, parent)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (tree *Bptree) Search(key Key) (uint32, error) {
	elem, err := tree.SearchAll(key)
	if err != nil {
		return 0, err
	}
	return elem.PgNumber, err
}

func (tree *Bptree) SearchAll(key Key) (elem Elem, err error) {
	// find paths
	paths, e := tree.findToExactElem(key)
	if e != nil && e != ERR_NOT_FOUND {
		return elem, e
	}
	if len(paths) == 0 {
		err = ERR_EMPTY
		return
	}
	node := paths[len(paths)-1]
	i, equal := node.Children.find(key)
	if !equal {
		i--
	}
	if i < 0 {
		err = ERR_NOT_FOUND
		return
	}
	elem = node.Children[i]
	return
}

func (tree *Bptree) find(key Key, idxAdjust func(*indexPage, int, bool) (int, error)) (paths []*indexPage, err error) {
	paths = make([]*indexPage, 0)

	node, err := tree.loadIndexPage(tree.rootPgNumber)
	if node == nil {
		return nil, ERR_EMPTY
	}
	if err != nil {
		return
	}
	if err != nil {
		return nil, err
	}

	for node != nil {
		paths = append(paths, node)

		elems := node.Children

		var isEqual bool = false

		idx := sort.Search(len(elems), func(i int) bool {
			isEqual = isEqual || (elems[i].Key == key)
			return elems[i].Key >= key
		})
		if idx, err = idxAdjust(node, idx, isEqual); err != nil {
			return
		}
		if node.Internal == 0 {
			break
		}
		if node, err = tree.loadIndexPage(elems[idx].PgNumber); err != nil {
			return
		}
	}
	return
}

func (tree *Bptree) findToInsert(key Key) (paths []*indexPage, err error) {
	return tree.find(key, func(node *indexPage, idx int, isEqual bool) (int, error) {
		if isEqual {
			return -1, ERR_OVERLAPPED
		}
		idx--
		if idx < 0 {
			idx = 0
		}
		return idx, nil
	})
}

func (tree *Bptree) findToExactElem(key Key) (paths []*indexPage, err error) {
	return tree.find(key, func(node *indexPage, idx int, isEqual bool) (int, error) {
		if !isEqual {
			idx--
			if idx < 0 {
				idx = 0
			}
		}
		return idx, nil
	})
}

func (tree *Bptree) updateParent(curr *indexPage, parent *indexPage) error {
	parentIdx, _ := parent.Children.find(curr.Last())
	parent.Children[parentIdx-1].Key = curr.Key()
	return tree.saveIndexPage(parent)
}

func (tree *Bptree) balance(paths []*indexPage) error {
	lenPaths := len(paths)

	if lenPaths == 0 {
		return ERR_EMPTY
	}

	var parent, curr, next *indexPage

	switch {
	case lenPaths == 1: // at root node
		rootPage := paths[0]
		newPageNumber, err1 := allocPage(tree.ctx)
		if err1 != nil {
			return err1
		}
		newPage := &indexPage{
			PgNumber:     newPageNumber,
			Children:     rootPage.Children,
			Internal:     0,
			PrevPgNumber: 0,
			NextPgNumber: 0,
		}
		rootPage.Internal = 1
		rootPage.Children = make([]Elem, 0, maxDegree+1)
		rootPage.insertElem(Elem{newPage.Key(), newPage.PgNumber}, 0, false)

		parent, curr = rootPage, newPage

	default:
		parent = paths[lenPaths-2]
		curr = paths[lenPaths-1]
	}

	currChildren := curr.Children
	mid := len(currChildren) / 2

	nextPgNumber, err2 := allocPage(tree.ctx)
	if err2 != nil {
		return err2
	}
	next = &indexPage{
		PgNumber:     nextPgNumber,
		Children:     make([]Elem, len(currChildren)-mid, maxDegree+1),
		Internal:     curr.Internal,
		NextPgNumber: curr.NextPgNumber,
		PrevPgNumber: curr.PgNumber,
	}

	curr.Children = currChildren[:mid]
	copy(next.Children, currChildren[mid:])
	curr.NextPgNumber = next.PgNumber

	if next.NextPgNumber != 0 {
		oldNext, errLoad := tree.loadIndexPage(next.NextPgNumber)
		if errLoad != nil {
			return errLoad
		}
		oldNext.PrevPgNumber = next.PgNumber
		errSave := tree.saveIndexPage(oldNext)
		if errSave != nil {
			return errSave
		}
	}

	err := parent.insertElem(Elem{next.Key(), next.PgNumber}, maxDegree, false)
	if err != nil {
		return err
	}
	err = tree.saveIndexPage(parent)
	if err != nil {
		return err
	}
	err = tree.saveIndexPage(next)
	if err != nil {
		return err
	}
	return tree.saveIndexPage(curr)
}

func (tree *Bptree) redistribution(paths []*indexPage, allowedDegree int) (bool, error) {
	lenPaths := len(paths)

	if lenPaths < 1 {
		panic("redistribution must not be in root")
	}

	var parent, curr *indexPage

	parent = paths[lenPaths-2]
	curr = paths[lenPaths-1]

	// get siblings
	lSibling, rSibling := tree.findSiblings(parent, curr.Last())

	var withLeft bool
	var lNode, rNode *indexPage
	var errl, errr error

	switch {
	case lSibling == 0 && rSibling == 0:
		panic("no such case")
	case lSibling != 0 && rSibling == 0:
		lNode, errl = tree.loadIndexPage(lSibling)
		if errl != nil {
			return false, errl
		}
		withLeft = true
	case lSibling == 0 && rSibling != 0:
		rNode, errr = tree.loadIndexPage(rSibling)
		if errr != nil {
			return false, errr
		}
		withLeft = false
	default:
		lNode, errl = tree.loadIndexPage(lSibling)
		if errl != nil {
			return false, errl
		}
		rNode, errr = tree.loadIndexPage(rSibling)
		if errr != nil {
			return false, errr
		}
		if len(lNode.Children) > len(rNode.Children) {
			withLeft = true
		} else {
			withLeft = false
		}
	}

	if withLeft {
		// redistribution with left sibling
		lsChildrenLen := len(lNode.Children)

		if lsChildrenLen-1 <= allowedDegree {
			return false, nil
		}

		borrow := lNode.Children[lsChildrenLen-1]
		lNode.Children = lNode.Children[:lsChildrenLen-1]

		newChildren := make([]Elem, len(curr.Children)+1, maxDegree+1)
		newChildren[0] = borrow
		copy(newChildren[1:], curr.Children)

		curr.Children = newChildren
		if errl = tree.saveIndexPage(lNode); errl != nil {
			return false, errl
		}
		if errl = tree.saveIndexPage(curr); errl != nil {
			return false, errl
		}
		if errl = tree.updateParent(curr, parent); errl != nil {
			return false, errl
		}
	} else {
		// redistribution with right sibling
		rsChildrenLen := len(rNode.Children)

		if rsChildrenLen-1 <= allowedDegree {
			return false, nil
		}

		borrow := rNode.Children[0]
		rNode.Children = rNode.Children[1:]

		curr.Children = append(curr.Children, borrow)
		if errr = tree.saveIndexPage(rNode); errr != nil {
			return false, errr
		}
		if errr = tree.saveIndexPage(curr); errr != nil {
			return false, errr
		}
		if errr = tree.updateParent(rNode, parent); errr != nil {
			return false, errr
		}
	}
	return true, nil
}

func (tree *Bptree) merge(paths []*indexPage) error {
	lenPaths := len(paths)

	if lenPaths < 1 {
		panic("redistribution must not be in root")
	}

	var parent, curr *indexPage

	parent = paths[lenPaths-2]
	curr = paths[lenPaths-1]

	// get siblings
	lSibling, rSibling := tree.findSiblings(parent, curr.Last())

	var withLeft bool
	var lNode, rNode *indexPage
	var errl, errr error

	switch {
	case lSibling == 0 && rSibling == 0:
		panic("no such case")
	case lSibling != 0 && rSibling == 0:
		lNode, errl = tree.loadIndexPage(lSibling)
		if errl != nil {
			return errl
		}
		withLeft = true
	case lSibling == 0 && rSibling != 0:
		rNode, errr = tree.loadIndexPage(rSibling)
		if errr != nil {
			return errr
		}
		withLeft = false
	default:
		lNode, errl = tree.loadIndexPage(lSibling)
		if errl != nil {
			return errl
		}
		rNode, errr = tree.loadIndexPage(rSibling)
		if errr != nil {
			return errr
		}
		if len(lNode.Children) > len(rNode.Children) {
			withLeft = true
		} else {
			withLeft = false
		}
	}

	if withLeft {
		// merging with left sibling
		if len(curr.Children)+len(lNode.Children) > maxDegree {
			panic("number of children must be after merging")
		}

		lNode.Children = append(lNode.Children, curr.Children...)
		lNode.NextPgNumber = curr.NextPgNumber

		if curr.NextPgNumber != 0 {
			nextPage, err1 := tree.loadIndexPage(curr.NextPgNumber)
			if err1 != nil {
				return err1
			}
			nextPage.PrevPgNumber = lNode.PgNumber
			err1 = tree.saveIndexPage(nextPage)
			if err1 != nil {
				return err1
			}
		}
		errl = freePage(tree.ctx, curr.PgNumber)
		if errl != nil {
			return errl
		}
		errl = tree.saveIndexPage(lNode)
		if errl != nil {
			return errl
		}
		parent.deleteElem(curr.Key(), maxDegree)
		errl = tree.saveIndexPage(parent)
		if errl != nil {
			return errl
		}
	} else {
		// merging with right sibling
		if len(rNode.Children)+len(curr.Children) > maxDegree {
			panic("number of children must be after merging")
		}

		rNode.Children = append(curr.Children, rNode.Children...)
		rNode.PrevPgNumber = curr.PrevPgNumber

		if curr.PrevPgNumber != 0 {
			prevPage, err1 := tree.loadIndexPage(curr.PrevPgNumber)
			if err1 != nil {
				return err1
			}
			prevPage.NextPgNumber = rNode.PgNumber
			err1 = tree.saveIndexPage(prevPage)
			if err1 != nil {
				return err1
			}
		}

		errr = freePage(tree.ctx, curr.PgNumber)
		if errr != nil {
			return errr
		}
		errr = tree.saveIndexPage(rNode)
		if errr != nil {
			return errr
		}
		parent.deleteElem(rNode.Key(), maxDegree)
		errr = tree.saveIndexPage(parent)
		if errr != nil {
			return errr
		}
	}
	return nil
}

func (tree *Bptree) findSiblings(parent *indexPage, key Key) (left, right uint32) {
	left = 0
	right = 0
	pChildrenLen := len(parent.Children)
	i, _ := parent.Children.find(key)
	i--
	if i != 0 {
		left = parent.Children[i-1].PgNumber
	}
	if i != pChildrenLen-1 {
		right = parent.Children[i+1].PgNumber
	}
	return
}
