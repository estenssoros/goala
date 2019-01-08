package goala

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	SQLLiteMemConn  = 0
	SqlLiteFileConn = iota
)

type Connection struct {
	ID      uuid.UUID
	DB      *sqlx.DB
	Dialect dialect
}

func (c *Connection) Close() {
	c.DB.Close()
}

func Connect(args ...string) (*Connection, error) {
	if len(args) > 1 {
		return nil, errors.New("connect only supports one argument")
	}
	switch len(args) {
	case 0:
		return connectDB(":memory:")
	case 1:
		return connectDB(args[0])
	default:
		return nil, errors.New("connect only supports one argument")
	}
}

// Ping wraps the db ping method
func (c *Connection) Ping() error {
	return c.DB.Ping()
}

func (c *Connection) initializeDB() error {
	id := uuid.Must(uuid.NewV4())
	if _, err := c.DB.Exec("CREATE TABLE goala_id (id TEXT)"); err != nil {
		return err
	}
	if _, err := c.DB.Exec("INSERT INTO goala_id (id) VALUES ('%s')", id.String()); err != nil {
		return err
	}
	c.ID = id
	return nil
}

func (c *Connection) checkID() error {
	id := uuid.UUID{}
	row := c.DB.QueryRow("SELECT * FROM goala_id")
	if err := row.Scan(&id); err != nil {
		return err
	}
	if id != c.ID {
		return errors.Errorf("data integrity: id mismatch: %s | %s", c.ID.String(), id.String())
	}
	return nil
}

func connectDB(connectionString string) (*Connection, error) {
	db, err := sqlx.Open("sqlite3", connectionString)
	if err != nil {
		return nil, errors.Wrapf(err, "connecting to %s", connectionString)
	}
	c := &Connection{
		DB:      db,
		Dialect: &sqlite3{},
	}
	if err := c.initializeDB(); err != nil {
		return nil, errors.Wrap(err, "initialize db")
	}
	return c, nil
}

// Query wraps the query method
func (c *Connection) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB.Query(query, args...)
}

// QueryContext wraps the QueryContext method
func (c *Connection) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return c.DB.QueryContext(ctx, query, args...)
}

// QueryRowContext wraps the QueryRowContext method
func (c *Connection) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return c.DB.QueryRowContext(ctx, query, args...)
}

// Exec wraps the ExecContext method
func (c *Connection) Exec(query string, args ...interface{}) (sql.Result, error) {
	return c.DB.Exec(query, args...)
}

// ExecContext wraps the ExecContext method
func (c *Connection) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return c.DB.ExecContext(ctx, query, args...)
}
