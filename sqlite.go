package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// SqliteDB implements DB interface for
type SqliteDB struct {
	*sql.DB
}

// ListTables lists all the tables in the database
func (db *SqliteDB) ListTables() ([]string, error) {
	const queryGetAllTablesQuery = `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';`
	rows, err := db.Query(queryGetAllTablesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

// ListRows returns row details given table name and
// offset in no particalar order
func (db *SqliteDB) ListRows(tableName string, offset, limit int) ([][]string, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d;", tableName, limit, offset)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	var result [][]string
	for rows.Next() {
		// Create a slice of interface{}'s to represent a row, and a slice of string pointers to scan into
		rowValues := make([]interface{}, len(cols))
		scanArgs := make([]*string, len(cols))
		for i := range rowValues {
			scanArgs[i] = new(string)
			rowValues[i] = scanArgs[i]
		}

		if err := rows.Scan(rowValues...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		currentRow := make([]string, len(cols))
		for i, val := range scanArgs {
			if val != nil {
				currentRow[i] = *val
			} else {
				// Handle NULL values as empty strings or choose another representation
				currentRow[i] = "" 
			}
		}
		result = append(result, currentRow)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

// Close does safe close of all the connections
func (db *SqliteDB) Close() {
	db.DB.Close()
	return
}

func (db *SqliteDB) query(q string) ([][]string, error) {
	rows, err := db.Query("select id, name from foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
	return nil, nil
}

// NewSqliteDB Creates a new db instance for the given sqlite file and returns it
// if the file does not exist ,not accessible or is not valid an error is returned
func NewSqliteDB(fileName string) (*SqliteDB, error) {
	db, err := sql.Open("sqlite3", fileName)
	if err != nil {
		return nil, err
	}

	return &SqliteDB{DB: db}, nil
}

func main() {
	db, err := NewSqliteDB("./foo.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	stmt, err := db.Prepare("select name from foo where id = ?")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	var name string
	err = stmt.QueryRow("3").Scan(&name)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(name)

	_, err = db.Exec("delete from foo")
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec("insert into foo(id, name) values(1, 'foo'), (2, 'bar'), (3, 'baz')")
	if err != nil {
		log.Fatal(err)
	}

	rows, err := db.Query("select id, name from foo")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(id, name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	db.Close()
	_db, err := sql.Open("sqlite3", "./test.db")
	if err != nil {
		log.Fatal(err)
	}
	db = &SqliteDB{DB: _db}
	defer db.Close()
	//rows, err = db.Query(getAllTablesQuery)
	rows, err = db.Query(`SELECT * FROM albums`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns() // Remember to check err afterwards
	vals := make([]interface{}, len(cols))
	for idx := range vals {
		vals[idx] = new(string)
	}
	for rows.Next() {
		err = rows.Scan(vals...)
		if err != nil {
			log.Fatal(err)
		}
		albStr := make([]string, 0)
		for _, v := range vals {
			albStr = append(albStr, *(v.(*string)))
		}
		fmt.Println(strings.Join(albStr, ","))
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
