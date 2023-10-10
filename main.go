package main

import (
	"fmt"

	"github.com/Shopify/go-lua"
)

func main() {
	fmt.Println("Hello")

	l := lua.NewState()
	lua.OpenLibraries(l)
	if err := lua.DoFile(l, "case1.lua"); err != nil {
		panic(err)
	}
}
