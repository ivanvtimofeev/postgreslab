package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/Shopify/go-lua"
	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

var wg sync.WaitGroup

type ConfDatabase struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"database"`
	Schema   string `yaml:"schema"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type Config struct {
	Database ConfDatabase `yaml:"database"`
	Text     string       `yaml:"text"`
}

// var Connection *pgx.Conn
var config *Config

func main() {

	config = readConfig()
	fmt.Printf("%#v", config)

	l := lua.NewState()

	registerStartAgent(l)

	lua.OpenLibraries(l)

	if err := lua.DoFile(l, "case1.lua"); err != nil {
		panic(err)
	}

	wg.Wait()
}

func connectDatabase(config *Config) (*pgx.Conn, error) {
	connStr := "postgres://" + config.Database.User + ":" + config.Database.Password + "@" + config.Database.Host + ":5432/" + config.Database.Database + "?sslmode=disable"
	return pgx.Connect(context.Background(), connStr)
}

func readConfig() *Config {
	buf, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	config := &Config{}
	err = yaml.Unmarshal(buf, config)
	if err != nil {
		panic(err)
	}
	return config
}

func startAgent(agentName string) {
	fmt.Println("StartAgent")
	l := lua.NewState()
	registerExecQuery(l)
	registerSleep(l)
	lua.OpenLibraries(l)
	if err := lua.DoFile(l, agentName); err != nil {
		panic(err)
	}
}

func registerStartAgent(l *lua.State) {
	l.Register("startAgent", func(l *lua.State) int {
		agentName := lua.CheckString(l, 1)
		agentsNo := lua.CheckNumber(l, 2)
		fmt.Println(agentName, agentsNo)
		for i := 1; i <= int(agentsNo); i++ {
			fmt.Println("Before start agent", i)
			wg.Add(1)
			go startAgent(agentName)
		}
		return 0
	})
}

func registerSleep(l *lua.State) {
	l.Register("sleep", func(l *lua.State) int {
		t := lua.CheckNumber(l, 1)
		time.Sleep(time.Duration(t) * time.Millisecond)
		return 0
	})
}

func registerExecQuery(l *lua.State) {
	l.Register("execQuery", func(l *lua.State) int {
		query := lua.CheckString(l, 1)

		conn, err := connectDatabase(config)
		if err != nil {
			panic(err)
		}
		defer conn.Close(context.Background())

		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			panic(err)
		}
		_ = rows.RawValues()
		return 0
	})
}
