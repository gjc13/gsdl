package main

import (
	"os"

	"github.com/gjc13/gsdl/frontend"
)

func main() {
	//for _, selectexp := range node.SelectExprs {
	//	fmt.Println(sqlparser.String(selectexp))
	//}
	//fmt.Println(sqlparser.String(node.Where))

	frontend.InputHandler(os.Stdin, frontend.MakeEngine())
}
