package frontend

import (
	"bufio"
	"fmt"
	"os"

	"github.com/xwb1989/sqlparser"
)

func InputHandler(f *os.File, e *Engine) {
	reader := bufio.NewReader(f)
	cnt := 0
	for {
		fmt.Printf("[%d] >", cnt)
		line := make([]byte, 0)
		var err error
		for {
			l := make([]byte, 0)
			line = append(line, '\n')
			l, _, err = reader.ReadLine()
			line = append(line, l...)
			if len(l) == 0 || l[len(l)-1] == ';' {
				break
			}
		}
		statement := string(line)
		if len(statement) > 0 && statement[len(statement)-1] == ';' {
			statement = statement[:len(statement)-1]
		}
		tree, parseErr := sqlparser.Parse(statement)
		if parseErr != nil {
			if err1 := e.MetaCommandHandler(statement); err1 != nil {
				fmt.Println(statement)
				fmt.Println(err1)
			}
		} else {
			if err1 := e.TableCommandHandler(statement, tree); err1 != nil {
				fmt.Println(statement)
				fmt.Println(err1)
			}
		}
		cnt++
		if err != nil {
			break
		}
	}
	if e.ctx != nil {
		e.ctx.EndUseDatabase()
		e.ctx = nil
	}
}
