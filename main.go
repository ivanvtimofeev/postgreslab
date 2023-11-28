package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	lua "github.com/yuin/gopher-lua"
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

var config *Config
var pool *pgxpool.Pool

func init() {
	config = readConfig()
	fmt.Printf("%#v", config)

	connStr := "postgres://" + config.Database.User + ":" + config.Database.Password + "@" + config.Database.Host + ":5432/" + config.Database.Database + "?sslmode=disable"

	var err error
	pool, err = pgxpool.New(context.Background(), connStr)

	if err != nil {
		panic(err.Error())
	}
}

func main() {

	l := lua.NewState()

	registerStartAgent(l)

	if err := l.DoFile("case1.lua"); err != nil {
		panic(err)
	}

	wg.Wait()
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
	if err := l.DoFile(agentName); err != nil {
		panic(err)
	}
}

func registerStartAgent(l *lua.LState) {
	l.Register("startAgent", func(l *lua.LState) int {
		agentName := l.CheckString(1)
		agentsNo := l.CheckNumber(2)
		fmt.Println(agentName, agentsNo)
		for i := 1; i <= int(agentsNo); i++ {
			fmt.Println("Before start agent", i)
			wg.Add(1)
			go startAgent(agentName)
		}
		return 0
	})
}

func registerSleep(l *lua.LState) {
	l.Register("sleep", func(l *lua.LState) int {
		t := l.CheckNumber(1)
		time.Sleep(time.Duration(t) * time.Millisecond)
		return 0
	})
}

func registerExecQuery(l *lua.LState) {
	l.Register("execQuery", func(l *lua.LState) int {
		query := l.CheckString(1)

		conn, err := pool.Acquire(context.Background())
		if err != nil {
			panic(err)
		}
		defer conn.Release()

		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			panic(err)
		}
		_ = rows.RawValues()
		return 0
	})
}
