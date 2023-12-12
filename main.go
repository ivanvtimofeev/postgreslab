package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
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

type TimetableRecord struct {
	Duration     int32 `yaml:"duration"` // phase duration in milliseconds
	AgentsAmount int32 `yaml:"agentsAmount"`
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
	registerStartAgentTimetable(l)

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

func parseDuration(dur string) int32 {
	units := dur[len(dur)-1:]
	var multipler int
	if units == "s" {
		multipler = 1
	} else if units == "m" {
		multipler = 60
	} else if units == "h" {
		multipler = 60 * 60
	} else {
		panic("Uncnown units " + units)
	}

	digit, err := strconv.Atoi(dur[:len(dur)-1])
	if err != nil {
		panic("Error convert duration " + err.Error())
	}

	return int32(digit) * int32(multipler)
}

func registerStartAgentTimetable(l *lua.LState) {
	l.Register("startAgentTimetable", func(l *lua.LState) int {
		timetable := []TimetableRecord{}

		agentName := l.CheckString(1)
		agentTimetable := l.CheckTable(2)
		fmt.Printf("\n")
		fmt.Println(agentName)
		agentTimetable.ForEach(func(key_str lua.LValue, value_str lua.LValue) { // Iterate over table rows
			value_str.(*lua.LTable).ForEach(func(key_col lua.LValue, value_col lua.LValue) {
				if key_col.String() == "1" { // Process Duration
					timetable = append(timetable, TimetableRecord{
						0,
						0,
					})
					duration := parseDuration(value_col.String())
					timetable[len(timetable)-1].Duration = duration
				} else if key_col.String() == "2" { // Process Workers amount
					amt, err := strconv.Atoi(value_col.String())
					if err != nil {
						panic("amount Conversion error " + err.Error())
					}
					timetable[len(timetable)-1].AgentsAmount = int32(amt)
				}
			})
		})

		fmt.Println("Timetable passed from startAgentTimetable lua call:")
		fmt.Println(timetable)
		// fmt.Printf("%+v", *agentTimetable)
		return 0
	})
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
