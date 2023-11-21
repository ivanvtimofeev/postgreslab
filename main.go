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

var Connection *pgx.Conn

func main() {

	config := readConfig()
	fmt.Printf("%#v", config)
	var err error
	Connection, err = connectDatabase(config)
	if err != nil {
		panic(err)
	}
	defer Connection.Close(context.Background())

	/* var greeting string
	err = conn.QueryRow(context.Background(), "select 'Hello, world!'").Scan(&greeting)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(greeting) */

	/* query := "SELECT flight_id, flight_no, departure_airport, arrival_airport FROM flights WHERE departure_airport = (SELECT departure_airport FROM flights GROUP BY departure_airport ORDER BY random() limit 1) ORDER BY scheduled_departure DESC LIMIT 30;"
	for i := 1; i < 10000; i++ {
		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			panic(err)
		}
		fmt.Println("================ iteration ===============")
		for rows.Next() {
			// rawValues := rows.RawValues()
			//fmt.Println(rawValues)
		}

	}
	os.Exit(0) */
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

func connectDatabase(config *Config) (*pgx.Conn, error) {
	connStr := "postgres://" + config.Database.User + ":" + config.Database.Password + "@" + config.Database.Host + ":5432/" + config.Database.Database + "?sslmode=disable"
	// fmt.Println(connStr)
	return pgx.Connect(context.Background(), connStr)
}

func readConfig() *Config {
	// Read config
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
		// fmt.Printf("Agent sleep for  %d seconds\n", int(t))
		time.Sleep(time.Duration(t) * time.Millisecond)
		return 0
	})
}

func registerExecQuery(l *lua.State) {
	l.Register("execQuery", func(l *lua.State) int {
		query := lua.CheckString(l, 1)

		// fmt.Println(query)
		config := readConfig()
		conn, err := connectDatabase(config)
		if err != nil {
			panic(err)
		}
		defer conn.Close(context.Background())

		rows, err := conn.Query(context.Background(), query)
		if err != nil {
			panic(err)
		}
		// fmt.Println("Next: ", rows.Next())
		_ = rows.RawValues()
		// fmt.Println("Values: ", rawValues)
		/*for rows.Next() {
			rawValues := rows.RawValues()
			fmt.Println("Values: ", rawValues)
		}*/
		return 0
	})
}
