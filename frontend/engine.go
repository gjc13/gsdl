package frontend

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	core "github.com/gjc13/gsdl/core"
	view "github.com/gjc13/gsdl/view"
	"github.com/xwb1989/sqlparser"
)

type Engine struct {
	nowDbName string
	ctx       *core.DbContext
}

func MakeEngine() *Engine {
	return &Engine{
		nowDbName: "",
		ctx:       nil,
	}
}

func (e *Engine) TableCommandHandler(rawStatement string, statement sqlparser.Statement) error {
	switch stmt := statement.(type) {
	case *sqlparser.CreateTable:
		return e.CreateTableHandler(stmt)
	case *sqlparser.Insert:
		return e.InsertHandler(stmt)
	case *sqlparser.Select:
		return e.SelectHandler(stmt)
	case *sqlparser.Delete:
		return e.DeleteHandler(stmt)
	case *sqlparser.Update:
		return e.UpdateHandler(stmt)
	default:
		return e.MetaCommandHandler(rawStatement)
	}
	return nil
}

func (e *Engine) MetaCommandHandler(statement string) error {
	statement = strings.Trim(statement, " \n\t")
	if len(statement) == 0 {
		return nil
	}
	stmt := strings.ToLower(statement)
	var err error
	switch {
	case strings.HasPrefix(stmt, "create database"):
		err = e.CreateDbHandler(strings.Trim(statement[15:], " "))
	case strings.HasPrefix(stmt, "drop database"):
		err = e.DropDbHandler(strings.Trim(statement[13:], " "))
	case strings.HasPrefix(stmt, "drop table"):
		err = e.DropTableHandler(strings.Trim(statement[10:], " "))
	case strings.HasPrefix(stmt, "use "):
		err = e.UseDbHandler(strings.Trim(statement[3:], " "))
	case strings.HasPrefix(stmt, "desc "):
		err = e.DescHandler(strings.Trim(statement[4:], " "))
	case stmt == "show tables":
		err = e.ShowTablesHandler()
	case stmt == "show databases":
		err = e.ShowDatabasesHandler()
	case stmt == "debug_print":
		v, err := e.ctx.CreateTableView("publisher")
		if err != nil {
			fmt.Println(err)
		} else {
			v.Print()
		}
	default:
		return ERR_STATEMENT
	}
	return err

}

func (e *Engine) DropDbHandler(dbname string) error {
	if e.ctx != nil {
		e.ctx.EndUseDatabase()
	}
	return os.Remove(dbname)
}

func (e *Engine) DropTableHandler(tableName string) error {
	if e.ctx == nil {
		return ERR_STATEMENT
		e.ctx.EndUseDatabase()
	}
	return e.ctx.DropTable(tableName)
}

func (e *Engine) UseDbHandler(dbname string) error {
	if len(dbname) == 0 {
		return ERR_NODB
	}
	ctx, err := core.StartUseDatabase(dbname)
	if err == nil {
		e.ctx = ctx
	} else {
		e.ctx = nil
	}
	return err
}

func (e *Engine) CreateDbHandler(dbname string) error {
	if len(dbname) == 0 {
		return ERR_NODB
	}
	return core.CreateDatabase(dbname)
}

func (e *Engine) ShowTablesHandler() error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	fmt.Println(e.ctx.GetTableNames())
	return nil
}

func (e *Engine) ShowDatabasesHandler() error {
	matches, _ := filepath.Glob("*.gsdl")
	for _, n := range matches {
		fmt.Printf("%v ", n[:len(n)-5])
	}
	fmt.Println()
	return nil
}

func (e *Engine) CreateTableHandler(stmt *sqlparser.CreateTable) error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	rowMeta := &core.RowMeta{
		FieldMetas:     make([]core.FieldMeta, 0),
		ClusterFieldId: 0,
	}
	tableName := string(stmt.Name)
	colNames := make([]string, 0)
	for i, colDef := range stmt.ColumnDefinitions {
		var fmeta core.FieldMeta
		types := strings.Split(colDef.ColType, "(")
		if len(types) != 2 {
			fmt.Println("Data width not given", colDef.ColName, colDef.ColType)
			return ERR_STATEMENT
		}
		width, _ := strconv.Atoi(types[1][:len(types[1])-1])
		switch types[0] {
		case "int":
			fmeta.DataType = core.INT_TYPE
			fmeta.FieldWidth = 8
		case "char":
			fallthrough
		case "varchar":
			fmeta.DataType = core.FIX_CHAR_TYPE
			fmeta.FieldWidth = uint16(width)
		default:
			fmt.Println("Data type not known", colDef.ColName, colDef.ColType)
			return ERR_STATEMENT
		}
		colNames = append(colNames, colDef.ColName)
		fmeta.Nullable = 1
		for _, att := range colDef.ColumnAtts {
			switch att {
			case "not null":
				fmeta.Nullable = 0
			case "primary key":
				rowMeta.ClusterFieldId = uint32(i)
				fmeta.Unique = 1
			case "unique key":
				fmeta.Unique = 1
			}
		}
		rowMeta.FieldMetas = append(rowMeta.FieldMetas, fmeta)
	}
	return e.ctx.CreateTable(tableName, colNames, rowMeta)
}

func (e *Engine) InsertHandler(stmt *sqlparser.Insert) error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	tableName := string(stmt.Table.Name)
	tableView, err := e.ctx.CreateTableView(tableName)
	if err != nil {
		return err
	}
	fieldNames := tableView.ColumnNames()
	values, ok := stmt.Rows.(sqlparser.Values)
	if !ok {
		return ERR_STATEMENT
	}
	for _, rowValue := range values {
		fieldValues, ok2 := rowValue.(sqlparser.ValTuple)
		if !ok2 {
			return ERR_STATEMENT
		}
		insertRow := make([]interface{}, 0)
		if len(fieldNames) != len(fieldValues) {
			return ERR_STATEMENT
		}
		for i, val := range fieldValues {
			if !e.isValueTypeCompatible(fieldNames[i], sqlparser.String(val)) {
				return ERR_FIELD
			}
			switch val.(type) {
			case sqlparser.NumVal:
				insertRow = append(insertRow, e.toCompatibleValue(fieldNames[i], sqlparser.String(val)))
			case sqlparser.StrVal:
				insertRow = append(insertRow, e.toCompatibleValue(fieldNames[i], sqlparser.String(val)))
			case *sqlparser.NullVal:
				insertRow = append(insertRow, nil)
			default:
				fmt.Println("Value type mismatch!")
				return ERR_STATEMENT
			}
		}
		errInsert := tableView.Insert(insertRow)
		if errInsert != nil {
			return errInsert
		}
	}
	return nil
}

func (e *Engine) SelectHandler(stmt *sqlparser.Select) error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	tableNames := make([]string, 0)
	colNames := make([]string, 0)
	isStar := false
	isReduce := false
	isGroup := (len(stmt.GroupBy) == 1)
	reduceColName := ""
	groupColName := ""
	reduceOpr := ""
	if isGroup {
		groupColName = sqlparser.String(stmt.GroupBy[0])
	}
	for _, v := range stmt.SelectExprs {
		switch v := v.(type) {
		case *sqlparser.StarExpr:
			isStar = true
			break
		case *sqlparser.NonStarExpr:
			name := sqlparser.String(v)
			idx := strings.Index(name, "(")
			if idx >= 0 {
				isReduce = true
				reduceColName = name[idx+1 : len(name)-1]
				reduceOpr = strings.ToLower(name[:idx])
				break
			} else {
				colNames = append(colNames, name)
			}
		}
	}
	for _, tableName := range stmt.From {
		tableNames = append(tableNames, sqlparser.String(tableName))
	}
	var err error
	var v view.Viewer
	if stmt.Where != nil {
		clause := e.boolExprToClause(stmt.Where.Expr)
		if clause == nil {
			return ERR_STATEMENT
		}
		v, err = e.orOfAndClausesToView(tableNames, clause.toOrOfAnds())
	} else {
		if len(tableNames) == 1 {
			rawView, err1 := e.ctx.CreateTableView(tableNames[0])
			if err1 != nil {
				return err1
			}
			v = view.MakeRawTableView(rawView)
		} else {
			return ERR_STATEMENT
		}
	}
	if err != nil {
		return err
	}
	if isReduce && isGroup {
		return e.groupReduceHandler(v, reduceOpr, groupColName, reduceColName)
	} else if isReduce {
		return e.reduceHandler(v, reduceOpr, reduceColName)
	} else {
		return e.printView(v, isStar, colNames)
	}
	return ERR_STATEMENT
}

func (e *Engine) printView(v view.Viewer, isStar bool, colNames []string) error {
	if !isStar {
		colIdxs := make([]int, 0, len(colNames))
		for _, n := range colNames {
			if !e.isColumnNameLegal(n, v) {
				return ERR_STATEMENT
			}
			colIdxs = append(colIdxs, view.ColumnName2Id(n, v.ColumnNames()))
		}
		fmt.Println(colNames)
		c := make(chan []interface{})
		go v.Iter(c)
		for row := range c {
			for _, idx := range colIdxs {
				fmt.Printf("%v, ", row[idx])
			}
			fmt.Println()
		}
	} else {
		view.PrintView(v)
	}
	return nil
}

func (e *Engine) reduceHandler(v view.Viewer, reduceOpr string, reduceColName string) error {
	fmt.Println(reduceOpr, reduceColName)
	if !e.isReduceColumnLegal(reduceColName, v) {
		return ERR_STATEMENT
	}
	fmt.Println(reduceColName)
	switch reduceOpr {
	case "min":
		fmt.Println(minReduceView(reduceColName, v))
	case "max":
		fmt.Println(maxReduceView(reduceColName, v))
	case "avg":
		sum, cnt := sumReduceView(reduceColName, v)
		fmt.Println(float64(sum) / float64(cnt))
	case "sum":
		sum, _ := sumReduceView(reduceColName, v)
		fmt.Println(sum)
	case "count":
		_, cnt := sumReduceView(reduceColName, v)
		fmt.Println(cnt)
	}
	return nil
}

func (e *Engine) groupReduceHandler(v view.Viewer, reduceOpr string, groupColName string, reduceColName string) error {
	if !e.isReduceColumnLegal(reduceColName, v) {
		return ERR_STATEMENT
	}
	if !e.isColumnNameLegal(groupColName, v) {
		return ERR_STATEMENT
	}
	fmt.Println(groupColName, reduceColName)
	switch reduceOpr {
	case "min":
		keys, vals := minGroupView(reduceColName, groupColName, v)
		for i := 0; i < len(keys); i++ {
			fmt.Printf("%v, %v\n", keys[i], vals[i])
		}
	case "max":
		keys, vals := maxGroupView(reduceColName, groupColName, v)
		for i := 0; i < len(keys); i++ {
			fmt.Printf("%v, %v\n", keys[i], vals[i])
		}
	case "avg":
		keys, sums, cnts := sumGroupView(reduceColName, groupColName, v)
		for i := 0; i < len(keys); i++ {
			fmt.Printf("%v, %v\n", keys[i], float64(sums[i])/float64(cnts[i]))
		}
	case "sum":
		keys, sums, _ := sumGroupView(reduceColName, groupColName, v)
		for i := 0; i < len(keys); i++ {
			fmt.Printf("%v, %v\n", keys[i], sums[i])
		}
	case "count":
		keys, _, cnts := sumGroupView(reduceColName, groupColName, v)
		for i := 0; i < len(keys); i++ {
			fmt.Printf("%v, %v\n", keys[i], cnts[i])
		}
	}
	return nil
}

func (e *Engine) DeleteHandler(stmt *sqlparser.Delete) error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	tableName := string(stmt.Table.Name)
	tableView, err := e.ctx.CreateTableView(tableName)
	if err != nil {
		return err
	}
	rows, err := e.whereExprToRows([]string{tableName}, stmt.Where)
	if err != nil {
		return err
	}
	for _, row := range rows {
		values := make([]core.FieldValue, 0, len(row))
		for i, field := range row {
			values = append(values, core.FieldValue{i, field})
		}
		if err := tableView.Delete(row[tableView.ClusterFieldId()], values); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) UpdateHandler(stmt *sqlparser.Update) error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	tableName := string(stmt.Table.Name)
	tableView, err := e.ctx.CreateTableView(tableName)
	if err != nil {
		return err
	}
	newValues := make([]core.FieldValue, 0)
	for _, updateExpr := range stmt.Exprs {
		colName := addTableName(tableName, sqlparser.String(updateExpr.Name))
		newValueStr := sqlparser.String(updateExpr.Expr)
		if !e.isValueTypeCompatible(colName, newValueStr) {
			return ERR_STATEMENT
		}
		newValues = append(newValues,
			core.FieldValue{
				view.ColumnName2Id(colName, tableView.ColumnNames()),
				e.toCompatibleValue(colName, newValueStr)})
	}
	rows, err := e.whereExprToRows([]string{tableName}, stmt.Where)
	for _, row := range rows {
		values := make([]core.FieldValue, 0, len(row))
		for i, field := range row {
			values = append(values, core.FieldValue{i, field})
		}
		if err := tableView.Update(row[tableView.ClusterFieldId()], values, newValues); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) whereExprToRows(tableNames []string, where *sqlparser.Where) ([][]interface{}, error) {
	var err error
	var v view.Viewer
	if where == nil {
		if len(tableNames) == 1 {
			rawView, err1 := e.ctx.CreateTableView(tableNames[0])
			if err1 != nil {
				return nil, err1
			}
			v = view.MakeRawTableView(rawView)
		} else {
			return nil, ERR_STATEMENT
		}
	} else {
		clause := e.boolExprToClause(where.Expr)
		if clause == nil {
			return nil, ERR_STATEMENT
		}
		v, err = e.orOfAndClausesToView(tableNames, clause.toOrOfAnds())
		if err != nil {
			return nil, err
		}
	}
	if v == nil {
		return nil, ERR_STATEMENT
	}
	res := make([][]interface{}, 0)
	c := make(chan []interface{})
	go v.Iter(c)
	for row := range c {
		res = append(res, row)
	}
	return res, nil
}

func (e *Engine) boolExprToClause(expr sqlparser.BoolExpr) Clauser {
	var c Clauser = nil
	switch expr := expr.(type) {
	case *sqlparser.AndExpr:
		c = &Clause{
			clauseType: AND_CLAUSE,
			clauses:    []Clauser{e.boolExprToClause(expr.Left), e.boolExprToClause(expr.Right)},
		}
	case *sqlparser.OrExpr:
		c = &Clause{
			clauseType: OR_CLAUSE,
			clauses:    []Clauser{e.boolExprToClause(expr.Left), e.boolExprToClause(expr.Right)},
		}
	case *sqlparser.NotExpr:
		c = &Clause{
			clauseType: NOT_CLAUSE,
			clauses:    []Clauser{e.boolExprToClause(expr.Expr)},
		}
	case *sqlparser.ComparisonExpr:
		c = &RawClause{
			condType: e.oprStringToValue(expr.Operator),
			lhs:      sqlparser.String(expr.Left),
			rhs:      sqlparser.String(expr.Right),
		}
	case *sqlparser.NullCheck:
		if expr.Operator == sqlparser.AST_IS_NULL {
			c = &RawClause{
				condType: view.COND_IS_NULL,
				lhs:      sqlparser.String(expr.Expr),
			}
		} else {
			c = &RawClause{
				condType: view.COND_ISNOT_NULL,
				lhs:      sqlparser.String(expr.Expr),
			}
		}
	case *sqlparser.ParenBoolExpr:
		return e.boolExprToClause(expr.Expr)
	default:
	}
	return c
}

func (e *Engine) oprStringToValue(opr string) int {
	switch opr {
	case sqlparser.AST_EQ:
		return view.COND_EQ
	case sqlparser.AST_NE:
		return view.COND_NE
	case sqlparser.AST_LT:
		return view.COND_L
	case sqlparser.AST_GT:
		return view.COND_G
	case sqlparser.AST_LE:
		return view.COND_LE
	case sqlparser.AST_GE:
		return view.COND_GE
	default:
		return -1
	}
}

func (e *Engine) DescHandler(tname string) error {
	if e.ctx == nil {
		return ERR_STATEMENT
	}
	tableNames := e.ctx.GetTableNames()
	rowMetas := e.ctx.GetTableMetas()
	for i, tableName := range tableNames {
		if tname == tableName {
			colNames, _ := e.getFieldNames(tableName)
			colMetas, _ := e.getFieldMetas(tableName)
			rowMeta := rowMetas[i]
			fmt.Printf("%s(cluster at %d)\n", tableName, rowMeta.ClusterFieldId)
			for j := 0; j < len(colNames); j++ {
				fmt.Printf("%s: ", colNames[j])
				meta := colMetas[j]
				switch meta.DataType {
				case core.INT_TYPE:
					fmt.Printf("INT ")
				case core.FLOAT_TYPE:
					fmt.Printf("FLOAT ")
				case core.FIX_CHAR_TYPE:
					fallthrough
				case core.VAR_CHAR_TYPE:
					fmt.Printf("VAR_CHAR ")
				}
				fmt.Printf("%d bytes nullable:%d unique:%d\n", meta.FieldWidth, meta.Nullable, meta.Unique)
			}
		}
	}
	return nil
}
