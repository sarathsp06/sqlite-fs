package main

// DB interface for all the basic database quesris
// for the filesystem to work
type DB interface {
	Closer
	CreateTable(
		tableName string,
		fields ...string,
	) error
	ListTables() ([]string, error)
	ListRows(
		tableName string,
		offset, limit int,
	) ([][]string, error)
}

// Closer intercaface defines mandatory functions for closable types
type Closer interface {
	Close()
}
