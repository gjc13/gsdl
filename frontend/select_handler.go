package frontend

import "github.com/gjc13/gsdl/view"

func (e *Engine) checkColumnName(tableNames []string, columnName string) bool {
	tableName, fieldName := divideColumnName(columnName)
	if !nameContains(tableNames, tableName) {
		return false
	}
	if _, err := e.getFieldMeta(tableName, fieldName); err != nil {
		return false
	}
	return true
}

func (e *Engine) checkClausesCompatible(tableNames []string, orOfAndClauses [][]RawClause) bool {
	for _, clauses := range orOfAndClauses {
		nJoin := 0
		for _, c := range clauses {
			if e.isColumnName(c.lhs) && !e.checkColumnName(tableNames, c.lhs) {
				return false
			}
			if e.isColumnName(c.rhs) && !e.checkColumnName(tableNames, c.rhs) {
				return false
			}
			if !e.isCompatible(c.lhs, c.rhs) {
				return false
			}
			if e.isColumnName(c.lhs) && e.isColumnName(c.rhs) && c.condType != view.COND_EQ {
				return false
			}
			if e.isColumnName(c.lhs) && e.isColumnName(c.rhs) {
				nJoin += 1
			}
		}
		if nJoin > 1 {
			return false
		}
	}
	return true
}

func (e *Engine) addTableNames(tableNames []string, orOfAndClauses [][]RawClause) {
	if len(tableNames) == 1 {
		for _, andClauses := range orOfAndClauses {
			for i, _ := range andClauses {
				if e.isColumnName(andClauses[i].lhs) {
					andClauses[i].lhs = addTableName(tableNames[0], andClauses[i].lhs)
				}
				if e.isColumnName(andClauses[i].rhs) {
					andClauses[i].rhs = addTableName(tableNames[0], andClauses[i].rhs)
				}
			}
		}
	}
}

func (e *Engine) unifyColumNamePlace(orOfAndClauses [][]RawClause) {
	for _, andClauses := range orOfAndClauses {
		for i, _ := range andClauses {
			if e.isColumnName(andClauses[i].rhs) && e.isConstant(andClauses[i].lhs) {
				andClauses[i].lhs, andClauses[i].rhs = andClauses[i].rhs, andClauses[i].lhs
			}
		}
	}
}

func (e *Engine) orOfAndClausesToView(tableNames []string, orOfAndClauses [][]RawClause) (view.Viewer, error) {
	e.addTableNames(tableNames, orOfAndClauses)
	e.unifyColumNamePlace(orOfAndClauses)
	if !e.checkClausesCompatible(tableNames, orOfAndClauses) {
		return nil, ERR_FIELD
	}
	if len(orOfAndClauses) == 0 {
		return nil, nil
	}
	v, err := e.andClausesToView(tableNames, orOfAndClauses[0])
	if err != nil {
		return nil, err
	}
	for i := 1; i < len(orOfAndClauses); i++ {
		v1, err1 := e.andClausesToView(tableNames, orOfAndClauses[i])
		if err1 != nil {
			return nil, err
		}
		v = view.MakeUnionView(v, v1)
	}
	return v, nil
}

func (e *Engine) andClausesToView(tableNames []string, andClauses []RawClause) (view.Viewer, error) {
	var baseView view.Viewer = nil
	if len(tableNames) == 1 {
		baseView, andClauses, _ = e.getDirectSearchView(tableNames, andClauses)
	} else {
		var baseTableName string
		baseView, andClauses, baseTableName = e.getDirectSearchView(tableNames, andClauses)
		baseView, andClauses = e.getJoinSearchView(andClauses, baseView, baseTableName)
	}
	if baseView == nil {
		return nil, ERR_STATEMENT
	}
	v := baseView
	for _, c := range andClauses {
		v = view.MakeFilterView(v, c.lhs, c.condType, e.toCompatibleValue(c.lhs, c.rhs))
	}
	return v, nil
}

func (e *Engine) getDirectSearchView(tableNames []string, andClauses []RawClause) (view.Viewer, []RawClause, string) {
	for i, c := range andClauses {
		if e.isConstantSearchClause(c) {
			tableName, _ := divideColumnName(c.lhs)
			baseView, err := e.ctx.CreateTableView(tableName)
			if err != nil {
				return nil, andClauses, ""
			}
			return view.MakeSearchRawTableView(baseView, c.lhs, e.toCompatibleValue(c.lhs, c.rhs)),
				append(andClauses[:i], andClauses[i+1:]...), tableName
		}
	}
	baseView, err := e.ctx.CreateTableView(tableNames[0])
	if err != nil {
		return nil, andClauses, ""
	}
	return view.MakeRawTableView(baseView), andClauses, tableNames[0]
}

func (e *Engine) getJoinSearchView(andClauses []RawClause, baseView view.Viewer, baseTableName string) (view.Viewer, []RawClause) {
	for i, c := range andClauses {
		if e.isColumnName(c.lhs) && e.isColumnName(c.rhs) {
			lTableName, _ := divideColumnName(c.lhs)
			rTableName, _ := divideColumnName(c.rhs)
			vl, errl := e.ctx.CreateTableView(lTableName)
			if errl != nil {
				return nil, andClauses
			}
			vr, errr := e.ctx.CreateTableView(rTableName)
			if errr != nil {
				return nil, andClauses
			}
			newClauses := append(andClauses[:i], andClauses[i+1:]...)
			switch {
			case lTableName == baseTableName:
				return view.MakeSearchJoinView(baseView, vr, c.lhs, c.rhs), newClauses
			case rTableName == baseTableName:
				return view.MakeSearchJoinView(baseView, vl, c.rhs, c.lhs), newClauses
			default:
				panic("Cannot join")
			}
		}
	}
	return nil, andClauses
}

func (e *Engine) isConstantSearchClause(clause RawClause) bool {
	return e.isConstantClause(clause) && clause.condType == view.COND_EQ && clause.rhs != "null"
}

func (e *Engine) isConstantClause(clause RawClause) bool {
	return e.isColumnName(clause.lhs) && e.isConstant(clause.rhs)
}
