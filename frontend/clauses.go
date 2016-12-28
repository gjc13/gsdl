package frontend

import "github.com/gjc13/gsdl/view"

const (
	NOT_CLAUSE int = iota
	AND_CLAUSE     = iota
	OR_CLAUSE      = iota
)

type Clauser interface {
	Not() Clauser
	removeNots() Clauser
	toOrOfAnds() [][]RawClause
}

type RawClause struct {
	condType int
	lhs      string
	rhs      string
}

type Clause struct {
	clauseType int
	clauses    []Clauser
}

func (c *RawClause) Not() Clauser {
	switch c.condType {
	case view.COND_EQ:
		c.condType = view.COND_NE
	case view.COND_NE:
		c.condType = view.COND_EQ
	case view.COND_L:
		c.condType = view.COND_GE
	case view.COND_LE:
		c.condType = view.COND_G
	case view.COND_G:
		c.condType = view.COND_LE
	case view.COND_GE:
		c.condType = view.COND_L
	case view.COND_IS_NULL:
		c.condType = view.COND_ISNOT_NULL
	case view.COND_ISNOT_NULL:
		c.condType = view.COND_IS_NULL
	default:
		panic("Wtf is this condition?")
	}
	return c
}

func (c *RawClause) toOrOfAnds() [][]RawClause {
	return [][]RawClause{[]RawClause{*c}}
}

func (c *RawClause) removeNots() Clauser {
	return c
}

func (c *Clause) Not() Clauser {
	switch c.clauseType {
	case NOT_CLAUSE:
		if len(c.clauses) != 1 {
			panic("Wrong not clause size")
		}
		return c.clauses[0]
	case OR_CLAUSE:
		return &Clause{
			clauseType: AND_CLAUSE,
			clauses:    c.notClauses(c.clauses),
		}
	case AND_CLAUSE:
		return &Clause{
			clauseType: OR_CLAUSE,
			clauses:    c.notClauses(c.clauses),
		}
	}
	return c
}

func (c *Clause) notClauses(clauses []Clauser) []Clauser {
	cs := make([]Clauser, 0, len(c.clauses))
	for _, subc := range c.clauses {
		cs = append(cs, subc.Not())
	}
	return cs
}

func (c *Clause) toOrOfAnds() [][]RawClause {
	c1 := c.removeNots()
	switch c1 := c1.(type) {
	case *RawClause:
		return c1.toOrOfAnds()
	case *Clause:
		c = c1
		switch c.clauseType {
		case OR_CLAUSE:
			if len(c.clauses) != 2 {
				panic("Wrong or clause size")
			}
			return append(c.clauses[0].toOrOfAnds(), c.clauses[1].toOrOfAnds()...)
		case AND_CLAUSE:
			if len(c.clauses) != 2 {
				panic("Wrong or clause size")
			}
			orClauses0 := c.clauses[0].toOrOfAnds()
			orClauses1 := c.clauses[1].toOrOfAnds()
			resClauses := make([][]RawClause, 0, len(orClauses0)*len(orClauses1))
			for _, c0 := range orClauses0 {
				for _, c1 := range orClauses1 {
					resClauses = append(resClauses, append(c0, c1...))
				}
			}
			return resClauses
		default:
			panic("Wrong clause type")
		}
	default:
		panic("Wrong clause type")
	}
}

func (c *Clause) removeNots() Clauser {
	if c.clauseType == NOT_CLAUSE {
		if len(c.clauses) != 1 {
			panic("Wrong not clause size")
		}
		notC := c.clauses[0]
		return notC.Not()
	} else {
		return c
	}
}
