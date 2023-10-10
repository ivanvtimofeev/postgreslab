package main

import (
	"fmt"

	"github.com/Shopify/go-lua"
)

func main() {
	fmt.Println("Hello")

	l := lua.NewState()
	registerStartAgent(l)
	lua.OpenLibraries(l)
	if err := lua.DoFile(l, "case1.lua"); err != nil {
		panic(err)
	}
}

func registerStartAgent(l *lua.State) {
	l.Register("startAgent", func(l *lua.State) int {
		agentsName := lua.CheckString(l, 1)
		agentsNo := lua.CheckNumber(l, 2)
		fmt.Println(agentsName, agentsNo)
		return 0
	})
}
