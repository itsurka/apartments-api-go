package dbhelper

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type DbConfig struct {
	Driver string
	Host   string
	Port   uint
	User   string
	Pass   string
	DbName string
}

func GetConnection(config DbConfig) (db *sql.DB) {
	//dbDriver := "postgres"
	//dbHost := "localhost"
	//dbPort := 5432
	//dbUser := "test"
	//dbPass := "test"
	//dbName := "go_web_parser"

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.User, config.Pass, config.DbName)

	db, err := sql.Open(config.Driver, psqlInfo)
	if err != nil {
		panic(err)
	}

	return db
}
