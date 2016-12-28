package core

import (
	"fmt"

	pager "github.com/gjc13/gsdl/pager"
)

func saveTableMetaPage(ctx *DbContext, page *tableMetaPage) error {
	wt, ok := ctx.transaction.(*pager.WriteTransaction)
	if !ok {
		panic("Cannot write when saving fix data page")
	}
	return wt.WritePage(page.PgNumber, page.toPageData())
}

func loadTableMetaPage(ctx *DbContext, pgNumber uint32) (*tableMetaPage, error) {
	rt := ctx.transaction.(pager.TransactionReader)
	data, err := rt.ReadPage(pgNumber)
	if err != nil {
		rt.AbortTransaction()
		return nil, err
	}
	return tableMetaPageFromData(pgNumber, data), nil
}

func createTable(ctx *DbContext, name string, columnNames []string, meta *RowMeta) (uint32, error) {
	page, err1 := createTableWithoutSecondIndex(ctx, name, columnNames, meta)
	if err1 != nil {
		return 0, err1
	}
	for i := 0; i < len(meta.FieldMetas); i++ {
		if i != int(meta.ClusterFieldId) {
			secondMeta := &RowMeta{
				FieldMetas:     []FieldMeta{meta.FieldMetas[i], meta.FieldMetas[meta.ClusterFieldId]},
				ClusterFieldId: 0,
			}
			secondMetaPage, err2 := createTableWithoutSecondIndex(ctx, fmt.Sprintf("%s:second%d", name, i),
				[]string{"", ""}, secondMeta)
			if err2 != nil {
				return 0, err2
			}
			page.FieldIndexPgNumbers[i] = secondMetaPage.PgNumber
		}
	}
	if err := saveTableMetaPage(ctx, page); err != nil {
		return 0, err
	}
	return page.PgNumber, nil
}

func createTableWithoutSecondIndex(ctx *DbContext, name string, columnNames []string, meta *RowMeta) (*tableMetaPage, error) {
	pgNumber, err1 := allocPage(ctx)
	if err1 != nil {
		return nil, err1
	}
	page := &tableMetaPage{
		PgNumber:              pgNumber,
		TableName:             name,
		RowInfo:               meta,
		ColumnNames:           columnNames,
		FirstDataPgNumber:     0,
		FieldIndexPgNumbers:   make([]uint32, len(meta.FieldMetas)),
		NextTableMetaPgNumber: 0,
		Dropped:               0,
	}
	tree, err2 := createTree(ctx)
	if err2 != nil {
		return nil, err2
	}
	page.FieldIndexPgNumbers[meta.ClusterFieldId] = tree.RootPgNumber()
	if err := saveTableMetaPage(ctx, page); err != nil {
		return nil, err
	}
	return page, nil
}
