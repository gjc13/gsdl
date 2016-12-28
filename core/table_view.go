package core

import (
	"fmt"
	"strings"

	pager "github.com/gjc13/gsdl/pager"
)

type TableView struct {
	ctx                   *DbContext
	metaPage              *tableMetaPage
	clusterFieldId        int
	mainIndexPgNumber     uint32
	secondIndexTableViews []*TableView
	nowPageNumber         uint32
	nowPageRowId          int
	nowPage               *fixDataPage
	tree                  *Bptree
}

func createView(ctx *DbContext, metaPgNumber uint32) (*TableView, error) {
	metaPage, err := loadTableMetaPage(ctx, metaPgNumber)
	if err != nil {
		return nil, err
	}
	view := &TableView{
		ctx:                   ctx,
		metaPage:              metaPage,
		clusterFieldId:        int(metaPage.RowInfo.ClusterFieldId),
		mainIndexPgNumber:     metaPage.FieldIndexPgNumbers[metaPage.RowInfo.ClusterFieldId],
		secondIndexTableViews: make([]*TableView, 0),
		nowPageNumber:         0,
		nowPageRowId:          0,
		nowPage:               nil,
		tree: &Bptree{
			ctx:          ctx,
			rootPgNumber: metaPage.FieldIndexPgNumbers[metaPage.RowInfo.ClusterFieldId],
		},
	}
	for i, pgNumber := range metaPage.FieldIndexPgNumbers {
		if i == view.clusterFieldId || metaPage.FieldIndexPgNumbers[i] == 0 {
			view.secondIndexTableViews = append(view.secondIndexTableViews, nil)
		} else {
			secondView, err1 := createView(ctx, pgNumber)
			if err1 != nil {
				return nil, err
			}
			view.secondIndexTableViews = append(view.secondIndexTableViews, secondView)
		}
	}
	return view, nil
}

func (view *TableView) ClusterFieldId() int {
	return view.clusterFieldId
}

func (view *TableView) ColumnNames() []string {
	names := make([]string, 0, len(view.metaPage.ColumnNames))
	for _, name := range view.metaPage.ColumnNames {
		names = append(names, strings.Join([]string{view.metaPage.TableName, name}, "."))
	}
	return names
}

func (view *TableView) ColumnMetas() []FieldMeta {
	metas := make([]FieldMeta, 0, len(view.metaPage.ColumnNames))
	for _, meta := range view.metaPage.RowInfo.FieldMetas {
		metas = append(metas, meta)
	}
	return metas
}

func (view *TableView) KeyStr(row []interface{}) string {
	return fmt.Sprintf("%v", row[view.clusterFieldId])
}

func (view *TableView) Print() {
	view.Reset()
	fmt.Println()
	fmt.Println(view.metaPage.TableName)
	cnt := 0
	for {
		n, ri := view.nowPageNumber, view.nowPageRowId
		r, err := view.Next()
		if err != nil {
			break
		}
		fmt.Printf("[%d<-%d.%d->%d] %v\n",
			view.nowPage.prevPgNumber, n, ri, view.nowPage.nextPgNumber, r)
		cnt++
	}
	fmt.Printf("%v rows\n", cnt)
	for _, v := range view.secondIndexTableViews {
		if v != nil {
			v.Print()
		}
	}
	view.Reset()
}

func (view *TableView) Search(fieldId int, key interface{}) ([][]interface{}, error) {
	if fieldId == view.clusterFieldId {
		return view.searchOnMainIndex(key, nil)
	} else {
		return view.searchOnSecondIndex(view.secondIndexTableViews[fieldId], fieldId, key)
	}
}

func (view *TableView) Insert(row []interface{}) error {
	//First check null
	for i, v := range row {
		if v == nil && !view.metaPage.RowInfo.FieldMetas[i].nullable() {
			return ERR_NIL
		}
	}
	//Check unique
	for i, fmeta := range view.metaPage.RowInfo.FieldMetas {
		if fmeta.Unique > 0 && row[i] != nil {
			res, err := view.Search(i, row[i])
			if err != nil {
				return err
			}
			if len(res) != 0 {
				return ERR_OVERLAPPED
			}
		}
	}
	return view.insert(row)
}

func (view *TableView) insert(row []interface{}) error {
	if view.metaPage.FirstDataPgNumber == 0 {
		firstPgNumber, err1 := allocPage(view.ctx)
		if err1 != nil {
			return err1
		}
		err2 := view.saveFixDataPage(&fixDataPage{
			pgNumber:     firstPgNumber,
			nextPgNumber: 0,
			prevPgNumber: 0,
			numRows:      0,
			meta:         view.metaPage.RowInfo,
			data:         make([]byte, 0),
		})
		if err2 != nil {
			return err2
		}
		view.metaPage.FirstDataPgNumber = firstPgNumber
		if err3 := saveTableMetaPage(view.ctx, view.metaPage); err3 != nil {
			return err3
		}
	}
	fmeta := view.metaPage.RowInfo.FieldMetas[view.clusterFieldId]
	view.nowPageNumber = view.metaPage.FirstDataPgNumber
	view.nowPageRowId = 0
	view.nowPage = nil
	var err error
	if row[view.clusterFieldId] != nil {
		if view.nowPageNumber, err = view.tree.Search(Key(fmeta.hash(row[view.clusterFieldId]))); err != nil && err != ERR_NOT_FOUND {
			return err
		}
		if err == ERR_NOT_FOUND {
			view.Reset()
		}
	}
	if err = view.moveToInsert(view.clusterFieldId, row[view.clusterFieldId]); err != nil {
		return err
	}
	if view.nowPage.canInsert() {
		fmeta := view.metaPage.RowInfo.FieldMetas[view.clusterFieldId]
		if view.nowPage.numRows == 0 ||
			fmeta.cmpField(row[view.clusterFieldId], view.nowPage.firstNonNullKeyField()) {
			if err = view.removeMainIndex(view.nowPage); err != nil {
				return err
			}
			view.nowPage.insertRow(row)
			if err = view.addMainIndex(view.nowPage); err != nil {
				return err
			}
		} else {
			view.nowPage.insertRow(row)
		}
		view.saveFixDataPage(view.nowPage)
	} else {
		newPgNumber, err1 := allocPage(view.ctx)
		if err1 != nil {
			return err1
		}
		var nextPage *fixDataPage = nil
		if view.nowPage.nextPgNumber != 0 {
			if nextPage, err = view.loadFixDataPage(view.nowPage.nextPgNumber); err != nil {
				return err
			}
		}
		newPage := &fixDataPage{
			pgNumber:     newPgNumber,
			nextPgNumber: view.nowPage.nextPgNumber,
			prevPgNumber: view.nowPage.pgNumber,
			meta:         view.nowPage.meta,
		}
		rowSize := uint32(newPage.meta.size())
		n := view.nowPage.numRows / 2
		newPage.numRows = view.nowPage.numRows - n
		newPage.data = make([]byte, rowSize*newPage.numRows)
		copy(newPage.data, view.nowPage.data[rowSize*n:])
		view.nowPage.numRows = n
		view.nowPage.data = view.nowPage.data[:rowSize*n]
		view.nowPage.nextPgNumber = newPage.pgNumber
		if nextPage != nil {
			nextPage.prevPgNumber = newPage.pgNumber
			if err = view.saveFixDataPage(nextPage); err != nil {
				return err
			}
		}
		if err = view.addMainIndex(newPage); err != nil {
			return err
		}
		if err = view.saveFixDataPage(view.nowPage); err != nil {
			return err
		}
		if err = view.saveFixDataPage(newPage); err != nil {
			return err
		}
		return view.insert(row)
	}
	// Update second index
	for i, view := range view.secondIndexTableViews {
		if view != nil && i != view.clusterFieldId {
			err1 := view.insert([]interface{}{
				row[i], row[view.clusterFieldId],
			})
			if err1 != nil {
				return err1
			}
		}
	}
	return nil
}

func (view *TableView) Update(key interface{}, values []FieldValue, newValues []FieldValue) error {
	foundRows, err := view.searchOnMainIndex(key, nil)
	if err != nil {
		return err
	}
	for _, row := range foundRows {
		if view.metaPage.RowInfo.checkRowSame(row, values) {
			if err1 := view.Delete(row[view.clusterFieldId], nil); err1 != nil {
				return err1
			}
			for _, v := range newValues {
				row[v.FieldId] = v.Value
			}
			if err1 := view.Insert(row); err1 != nil {
				return err1
			}
		}
	}
	return nil
}

func (view *TableView) forMainIdxConcerned(key interface{}, handler func(*fixDataPage, [][]interface{}) error) error {
	fmeta := view.metaPage.RowInfo.FieldMetas[view.clusterFieldId]
	view.Reset()
	var err error
	if key != nil {
		view.nowPageNumber, err = view.tree.Search(Key(fmeta.hash(key)))
		if err == ERR_NOT_FOUND {
			return nil
		} else if err != nil {
			return err
		}
	}
	if view.nowPage, err = view.loadFixDataPage(view.nowPageNumber); err != nil {
		return err
	}
	nowPage := view.nowPage
	for {
		rows := nowPage.getRows(key)
		if err = handler(nowPage, rows); err != nil {
			return err
		}
		if len(rows) == 0 || nowPage.nextPgNumber == 0 {
			break
		}
		if nowPage, err = view.loadFixDataPage(nowPage.nextPgNumber); err != nil {
			return err
		}
	}
	nowPage = view.nowPage
	for {
		if nowPage.prevPgNumber == 0 {
			break
		}
		if nowPage, err = view.loadFixDataPage(nowPage.prevPgNumber); err != nil {
			return err
		}
		rows := nowPage.getRows(key)
		if len(rows) == 0 {
			break
		}
		if err = handler(nowPage, rows); err != nil {
			return err
		}
	}
	return nil
}

func (view *TableView) Delete(key interface{}, values []FieldValue) error {
	var lastPage *fixDataPage = nil
	return view.forMainIdxConcerned(key, func(page *fixDataPage, rows [][]interface{}) error {
		var err error
		if err = view.removeMainIndex(page); err != nil {
			return err
		}
		if values != nil {
			page.deleteWithFields(key, values)
		} else {
			page.deleteRow(key)
		}
		for _, row := range rows {
			if values == nil || view.metaPage.RowInfo.checkRowSame(row, values) {
				for i, v := range view.secondIndexTableViews {
					if v != nil {
						v.Delete(row[i], []FieldValue{{1, row[view.clusterFieldId]}})
					}
				}
			}
		}
		if lastPage != nil {
			if err = view.addMainIndex(lastPage); err != nil {
				return err
			}
		}
		if page.numRows == 0 {
			lastPage = page
			return view.deleteFixDataPage(page)
		}
		if err = view.addMainIndex(page); err != nil {
			return err
		}
		lastPage = page
		return view.saveFixDataPage(page)
	})
}

func (view *TableView) searchOnMainIndex(key interface{}, values []FieldValue) ([][]interface{}, error) {
	resultRows := make([][]interface{}, 0)
	err := view.forMainIdxConcerned(key, func(page *fixDataPage, rows [][]interface{}) error {
		if values == nil {
			resultRows = append(resultRows, rows...)
		} else {
			for _, row := range rows {
				if view.metaPage.RowInfo.checkRowSame(row, values) {
					resultRows = append(resultRows, row)
				}
			}
		}
		return nil
	})
	return resultRows, err
}

func (view *TableView) searchOnSecondIndex(indexView *TableView, fieldId int, key interface{}) ([][]interface{}, error) {
	fmeta := view.metaPage.RowInfo.FieldMetas[fieldId]
	if indexView == nil {
		panic("Second index not found")
	}
	mainIdxRows, err := indexView.searchOnMainIndex(key, nil)
	if err != nil {
		return nil, err
	}
	resultRows := make([][]interface{}, 0)
	for _, mainIdxRow := range mainIdxRows {
		values := []FieldValue{
			{fieldId, mainIdxRow[0]},
			{view.clusterFieldId, mainIdxRow[1]},
		}
		if view.metaPage.RowInfo.FieldMetas[view.clusterFieldId].Unique != 0 {
			values = nil
		}
		rows, err1 := view.searchOnMainIndex(mainIdxRow[1], values)
		if err1 != nil {
			return nil, err1
		}
		for _, row := range rows {
			if fmeta.isEqual(row[fieldId], key) {
				resultRows = append(resultRows, row)
			}
		}
	}
	return resultRows, nil
}

func (view *TableView) addMainIndex(page *fixDataPage) error {
	fmeta := view.metaPage.RowInfo.FieldMetas[view.clusterFieldId]
	keyField := page.firstNonNullKeyField()
	if keyField == nil {
		return nil
	}
	keyHash := Key(fmeta.hash(keyField))
	elem, err := view.tree.SearchAll(keyHash)
	if err != nil && err != ERR_NOT_FOUND {
		return err
	}
	if err == nil && elem.Key == keyHash {
		if page.pgNumber < elem.PgNumber {
			if err1 := view.tree.Remove(keyHash); err1 != nil && err1 != ERR_NOT_FOUND {
				return err1
			}
		} else {
			return nil
		}
	}
	view.tree.Insert(Elem{keyHash, page.pgNumber})
	return nil
}

func (view *TableView) removeMainIndex(page *fixDataPage) error {
	if page.numRows == 0 {
		return nil
	}
	fmeta := view.metaPage.RowInfo.FieldMetas[view.clusterFieldId]
	keyField := page.firstNonNullKeyField()
	if keyField == nil {
		return nil
	}
	keyHash := Key(fmeta.hash(keyField))
	return view.tree.Remove(keyHash)
}

func (view *TableView) Next() ([]interface{}, error) {
	var err error
	var hn bool
	hn, err = view.HasNext()
	if err != nil {
		return nil, err
	}
	if !hn {
		return nil, ERR_END_ITER
	}
	result := view.nowPage.getRowAt(view.nowPageRowId)
	view.nowPageRowId++
	if view.nowPageRowId == int(view.nowPage.numRows) && view.nowPage.nextPgNumber != 0 {
		view.nowPageNumber = view.nowPage.nextPgNumber
		view.nowPageRowId = 0
		if view.nowPage, err = view.loadFixDataPage(view.nowPageNumber); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (view *TableView) HasNext() (bool, error) {
	var err error
	if view.nowPage == nil {
		if view.nowPage, err = view.loadFixDataPage(view.nowPageNumber); err != nil {
			return false, err
		}
	}
	return view.hasNext(), nil
}

func (view *TableView) Reset() {
	view.nowPage = nil
	view.nowPageNumber = view.metaPage.FirstDataPgNumber
	view.nowPageRowId = 0
}

func (view *TableView) hasNext() bool {
	return view.nowPage.nextPgNumber != 0 || view.nowPageRowId != int(view.nowPage.numRows)
}

func (view *TableView) moveToInsert(fieldId int, key interface{}) error {
	fmeta := view.metaPage.RowInfo.FieldMetas[fieldId]
	var err error
	if view.nowPage, err = view.loadFixDataPage(view.nowPageNumber); err != nil {
		return err
	}
	for {
		if view.nowPage, err = view.loadFixDataPage(view.nowPageNumber); err != nil {
			return err
		}
		if key == nil || view.nowPage.numRows == 0 || view.nowPage.nextPgNumber == 0 {
			return nil
		}
		if !fmeta.cmpField(view.nowPage.lastKeyField(), key) {
			return nil
		}
		view.nowPageNumber = view.nowPage.nextPgNumber
	}
	return nil
}

func (view *TableView) loadFixDataPage(pgNumber uint32) (*fixDataPage, error) {
	rt := view.ctx.transaction.(pager.TransactionReader)
	data, err := rt.ReadPage(pgNumber)
	if err != nil {
		rt.AbortTransaction()
		return nil, err
	}
	return fixDataPageFromData(pgNumber, view.metaPage.RowInfo, data), nil
}

func (view *TableView) saveFixDataPage(page *fixDataPage) error {
	wt, ok := view.ctx.transaction.(*pager.WriteTransaction)
	if !ok {
		panic("Cannot write when saving fix data page")
	}
	return wt.WritePage(page.pgNumber, page.toPageData())
}

func (view *TableView) deleteFixDataPage(page *fixDataPage) error {
	freePage(view.ctx, page.pgNumber)
	if page.prevPgNumber != 0 {
		prevPage, err := view.loadFixDataPage(page.prevPgNumber)
		if err != nil {
			return err
		}
		prevPage.nextPgNumber = page.nextPgNumber
		if err1 := view.saveFixDataPage(prevPage); err1 != nil {
			return err1
		}
	}
	if page.nextPgNumber != 0 {
		nextPage, err := view.loadFixDataPage(page.nextPgNumber)
		if err != nil {
			return err
		}
		nextPage.prevPgNumber = page.prevPgNumber
		if err1 := view.saveFixDataPage(nextPage); err1 != nil {
			return err1
		}
	}
	if page.pgNumber == view.metaPage.FirstDataPgNumber {
		view.metaPage.FirstDataPgNumber = page.nextPgNumber
		return saveTableMetaPage(view.ctx, view.metaPage)
	}
	return nil
}
