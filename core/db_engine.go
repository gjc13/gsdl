package core

import pager "github.com/gjc13/gsdl/pager"

func CreateDatabase(filename string) error {
	wt := &pager.WriteTransaction{}
	wt.StartTransaction(filename + ".gsdl")
	page := &dbMetaPage{
		PageNumber:               0,
		FirstTableMetaPageNumber: 0,
	}
	if err := wt.WritePage(0, page.toPageData()); err != nil {
		return err
	}
	if err := wt.EndTransaction(); err != nil {
		return err
	}
	return nil
}

func StartUseDatabase(filename string) (*DbContext, error) {
	ctx := &DbContext{
		transaction: &pager.WriteTransaction{},
	}
	ctx.transaction.StartTransaction(filename + ".gsdl")
	rt := ctx.transaction.(pager.TransactionReader)
	data, err := rt.ReadPage(0)
	if err != nil {
		rt.AbortTransaction()
		return nil, err
	}
	ctx.metaPage = dbMetaPageFromPageData(0, data)
	return ctx, nil
}

func (ctx *DbContext) EndUseDatabase() error {
	return ctx.transaction.EndTransaction()
}

func (ctx *DbContext) CreateTable(name string, columnNames []string, meta *RowMeta) error {
	wt, ok := ctx.transaction.(*pager.WriteTransaction)
	if !ok {
		panic("Cannot write when creating table")
	}
	oldPage, err1 := ctx.findTableMetaWithName(name)
	if err1 != ERR_NOT_FOUND {
		return ERR_OVERLAPPED
	}
	newPageNumber, err2 := createTable(ctx, name, columnNames, meta)
	if err2 != nil {
		return err2
	}
	if ctx.metaPage.FirstTableMetaPageNumber == 0 {
		ctx.metaPage.FirstTableMetaPageNumber = newPageNumber
		return wt.WritePage(0, ctx.metaPage.toPageData())
	} else {
		oldPage.NextTableMetaPgNumber = newPageNumber
		return saveTableMetaPage(ctx, oldPage)
	}
}

func (ctx *DbContext) GetTableNames() []string {
	nowPgNumber := ctx.metaPage.FirstTableMetaPageNumber
	var metaPage *tableMetaPage
	var err error
	var result []string = make([]string, 0)
	for nowPgNumber != 0 {
		metaPage, err = loadTableMetaPage(ctx, nowPgNumber)
		if err != nil {
			return []string{}
		}
		result = append(result, metaPage.TableName)
		nowPgNumber = metaPage.NextTableMetaPgNumber
	}
	return result
}

func (ctx *DbContext) GetTableMetas() []*RowMeta {
	nowPgNumber := ctx.metaPage.FirstTableMetaPageNumber
	var metaPage *tableMetaPage
	var err error
	result := make([]*RowMeta, 0)
	for nowPgNumber != 0 {
		metaPage, err = loadTableMetaPage(ctx, nowPgNumber)
		if err != nil {
			return []*RowMeta{}
		}
		result = append(result, metaPage.RowInfo)
		nowPgNumber = metaPage.NextTableMetaPgNumber
	}
	return result
}

func (ctx *DbContext) DropTable(name string) error {
	oldPage, _ := ctx.findTableMetaWithName(name)
	if oldPage == nil {
		return ERR_NOT_FOUND
	}
	oldPage.Dropped = 1
	return saveTableMetaPage(ctx, oldPage)
}

func (ctx *DbContext) CreateTableView(name string) (*TableView, error) {
	tMetaPage, _ := ctx.findTableMetaWithName(name)
	if tMetaPage == nil {
		return nil, ERR_NOT_FOUND
	}
	return createView(ctx, tMetaPage.PgNumber)
}

func (ctx *DbContext) findTableMetaWithName(name string) (*tableMetaPage, error) {
	nowPgNumber := ctx.metaPage.FirstTableMetaPageNumber
	var metaPage *tableMetaPage
	var err error
	for nowPgNumber != 0 {
		metaPage, err = loadTableMetaPage(ctx, nowPgNumber)
		if err != nil {
			return nil, err
		}
		if metaPage.TableName == name && !metaPage.dropped() {
			return metaPage, nil
		}
		nowPgNumber = metaPage.NextTableMetaPgNumber
	}
	return metaPage, ERR_NOT_FOUND
}
