package main

import (
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"database/sql"
	"github.com/Shopify/go-lua"
	_ "github.com/lib/pq"
	"gopkg.in/yaml.v3"
)

var wg sync.WaitGroup

type ConfDatabase struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	Database string `yaml:"databse"`
	Schema   string `yaml:"schema"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type Config struct {
	Database ConfDatabase `yaml:"database"`
	Text     string       `yaml:"text"`
}

var db *sql.DB

func main() {

	config := readConfig()
	err := connectDatabase(config)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%#v", config)

	// Start base scenario

	l := lua.NewState()

	// Add functions to lua
	registerStartAgent(l)

	lua.OpenLibraries(l)

	if err := lua.DoFile(l, "case1.lua"); err != nil {
		panic(err)
	}

	// time.Sleep((20 * time.Second))
	wg.Wait()
}

func connectDatabase(config *Config) error {
	connStr := "postgres://" + config.Database.User + ":" + config.Database.Password + "@" + config.Database.Host + "/" + config.Database.Database + "?sslmode=disable"
	db, err := sql.Open("postgres", connStr)

	if err != nil {
		return err
	}

	if err = db.Ping(); err != nil {
		return err
	}
	// this will be printed in the terminal, confirming the connection to the database
	fmt.Println("The database is connected")

	return nil
}

func readConfig() *Config {
	// Read config
	buf, err := ioutil.ReadFile("config.yaml")
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
		fmt.Printf("Agent sleep for  %d seconds\n", int(t))
		time.Sleep(time.Duration(t) * time.Second)
		return 0
	})
}

func registerExecQuery(l *lua.State) {
	l.Register("execQuery", func(l *lua.State) int {
		query := lua.CheckString(l, 1)
		fmt.Println(query)
		return 0
	})
}
