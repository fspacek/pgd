package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/fatih/color"
	_ "github.com/lib/pq"
)

func main() {
	var connStr string
	var query string
	flag.StringVar(&connStr, "c", "postgres://postgres:postgres@localhost?sslmode=disable", "postgres connection string")
	flag.StringVar(&query, "q", "select 1", "select query in form of select filename, blob from table")
	flag.Parse()

	green := color.New(color.FgGreen).SprintFunc()
	fmt.Printf("Opening connection to database %s\n", green(connStr))
	db, err := sql.Open("postgres", connStr)
	checkErr(err)
	defer db.Close()

	fmt.Printf("Executing query %s\n\n", green(query))
	rows, err := db.Query(query)
	checkErr(err)

	cols, err := rows.Columns()
	checkErr(err)

	if len(cols) != 2 {
		panic("Exactly two columns are expected, one name for file and one with blob data")
	}

	fmt.Println("Processing rows...")
	for rows.Next() {
		var name string
		var content sql.RawBytes
		err = rows.Scan(&name, &content)
		checkErr(err)

		fmt.Printf("Writing blob to file %s\n", green(name))

		err = ioutil.WriteFile(name, content, 0644)
		checkErr(err)
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
