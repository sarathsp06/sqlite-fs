package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"golang.org/x/net/context"
	"strconv"
	"strings"
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [options] MOUNTPOINT\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Options:\n")
	flag.PrintDefaults()
}

func main() {
	dbfilePath := flag.String("dbfile", "./foo.db", "Path to the SQLite database file")
	flag.Usage = usage
	flag.Parse()

	if flag.NArg() != 1 {
		usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("helloworld"),
		fuse.Subtype("hellofs"),
		fuse.LocalVolume(),
		fuse.VolumeName("Hello world!"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// Initialize SqliteDB
	log.Printf("Using database file: %s", *dbfilePath)
	db, errDb := NewSqliteDB(*dbfilePath)
	if errDb != nil {
		log.Fatalf("Failed to initialize database '%s': %v", *dbfilePath, errDb)
	}
	defer db.Close()

	err = fs.Serve(c, FS{db: db})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct {
	db *SqliteDB
}

func (f FS) Root() (fs.Node, error) {
	return Dir{db: f.db}, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	db *SqliteDB
}

func (d Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1 // Root directory inode
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	tables, err := d.db.ListTables()
	if err != nil {
		log.Printf("Error listing tables in Dir.Lookup: %v", err)
		return nil, fuse.EIO // Return I/O error to indicate backend issue
	}
	for _, tableName := range tables {
		if tableName == name {
			return TableDir{db: d.db, tableName: tableName}, nil
		}
	}
	return nil, fuse.ENOENT
}

func (d Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	tables, err := d.db.ListTables()
	if err != nil {
		log.Printf("Error listing tables in Dir.ReadDirAll: %v", err)
		return nil, fuse.EIO // Return I/O error
	}

	var entries []fuse.Dirent
	for i, tableName := range tables {
		entries = append(entries, fuse.Dirent{
			Inode: uint64(i + 2), // Inodes 0 and 1 are typically special (0=invalid, 1=root)
			Name:  tableName,
			Type:  fuse.DT_Dir,
		})
	}
	return entries, nil
}

// TableDir implements Node for a directory representing a single DB table.
type TableDir struct {
	db        *SqliteDB
	tableName string
}

func (td TableDir) Attr(ctx context.Context, a *fuse.Attr) error {
	// Find a unique inode for the table directory.
	// Simple hashing for demonstration. For a real FS, ensure uniqueness and persistence.
	// Adding a prime number to avoid collision with root inode (1) and other potential fixed inodes.
	var inode uint64
	for _, char := range td.tableName {
		inode = inode*31 + uint64(char)
	}
	inode += 1000 

	a.Inode = inode
	a.Mode = os.ModeDir | 0555 // Readable and executable directory
	return nil
}

// Lookup for TableDir will eventually find files representing rows or operations.
// Lookup for TableDir will find files representing rows.
func (td TableDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	rowIndex, err := strconv.Atoi(name)
	if err != nil {
		// Not a valid row index number
		return nil, fuse.ENOENT
	}
	if rowIndex < 0 {
		return nil, fuse.ENOENT
	}

	// Check if the row exists by trying to fetch it.
	// We ask for 1 row at the specific offset (rowIndex).
	rows, err := td.db.ListRows(td.tableName, rowIndex, 1)
	if err != nil {
		log.Printf("Error looking up row %d in table %s: %v", rowIndex, td.tableName, err)
		return nil, fuse.EIO // I/O error if database access fails
	}
	if len(rows) == 0 {
		// Row does not exist
		return nil, fuse.ENOENT
	}

	return RowFile{db: td.db, tableName: td.tableName, rowIndex: rowIndex}, nil
}

// ReadDirAll for TableDir lists rows as files.
// Each file is named by its 0-based index.
func (td TableDir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	// Fetch all rows (or a large number of them)
	// Assuming ListRows can handle a large limit.
	// A more robust solution might involve pagination if tables are huge.
	rows, err := td.db.ListRows(td.tableName, 0, 10000) // Limit to 10000 rows for now
	if err != nil {
		log.Printf("Error listing rows for table %s: %v", td.tableName, err)
		return nil, fuse.EIO // I/O error
	}

	var entries []fuse.Dirent
	// Calculate a base inode for the table to try and ensure unique inodes for rows.
	// This is a simplified approach. A robust FS would need a more solid inode generation strategy.
	var tableBaseInode uint64
	for _, char := range td.tableName {
		tableBaseInode = tableBaseInode*31 + uint64(char)
	}
	tableBaseInode += 2000 // Offset to avoid collision with table dir inodes and root/other fixed inodes.

	for i := range rows {
		entries = append(entries, fuse.Dirent{
			Inode: tableBaseInode + uint64(i) + 1, // Ensure inode is > 0 and unique within this dir
			Name:  strconv.Itoa(i),
			Type:  fuse.DT_File,
		})
	}
	return entries, nil
}

// RowFile implements Node for a file representing a single row in a table.
// For now, it's a stub.
type RowFile struct {
	db        *SqliteDB
	tableName string
	rowIndex  int
}

// Attr provides attributes for a RowFile.
func (rf RowFile) Attr(ctx context.Context, a *fuse.Attr) error {
	rows, err := rf.db.ListRows(rf.tableName, rf.rowIndex, 1)
	if err != nil {
		log.Printf("Error fetching row for Attr (table: %s, row: %d): %v", rf.tableName, rf.rowIndex, err)
		return fuse.EIO
	}
	if len(rows) == 0 {
		return fuse.ENOENT
	}

	rowData := rows[0]
	contentString := strings.Join(rowData, ",") + "\n"

	// Inode calculation (consistent with TableDir.ReadDirAll and previous stub)
	var tableBaseInode uint64
	for _, char := range rf.tableName {
		tableBaseInode = tableBaseInode*31 + uint64(char)
	}
	tableBaseInode += 2000 // Offset to avoid collision

	a.Inode = tableBaseInode + uint64(rf.rowIndex) + 1
	a.Mode = 0444 // Read-only file
	a.Size = uint64(len(contentString))
	return nil
}

// ReadAll reads the content of a RowFile.
func (rf RowFile) ReadAll(ctx context.Context) ([]byte, error) {
	rows, err := rf.db.ListRows(rf.tableName, rf.rowIndex, 1)
	if err != nil {
		log.Printf("Error fetching row for ReadAll (table: %s, row: %d): %v", rf.tableName, rf.rowIndex, err)
		return nil, fuse.EIO
	}
	if len(rows) == 0 {
		return nil, fuse.ENOENT
	}

	rowData := rows[0]
	contentString := strings.Join(rowData, ",") + "\n"
	return []byte(contentString), nil
}
