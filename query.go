package goala

import (
	"fmt"
	"strings"
)

// Query is the main value that is used to build up a query
// to be executed against the `Connection`.
type Query struct {
	RawSQL       *clause
	limitResults int
	addColumns   []string
	whereClauses clauses
	orderClauses clauses
	fromClauses  fromClauses
	Connection   *Connection
}

// First wraps first query
func (c *Connection) First(model interface{}) error {
	return Q(c).First(model)
}

// First executes select one
func (q *Query) First(model interface{}) error {
	q.Limit(1)
	m := &Model{Value: model}
	if err := q.Connection.Dialect.SelectOne(q.Connection.DB, m, *q); err != nil {
		return err
	}
	return nil
}

// RawQuery will override the query building feature and will use
// whatever query you want to execute against the `Connection`. You can continue
// to use the `?` argument syntax.
//
//	c.RawQuery("select * from foo where id = ?", 1)
func (c *Connection) RawQuery(stmt string, args ...interface{}) *Query {
	return Q(c).RawQuery(stmt, args...)
}

// RawQuery will override the query building feature and will use
// whatever query you want to execute against the `Connection`. You can continue
// to use the `?` argument syntax.
//
//	q.RawQuery("select * from foo where id = ?", 1)
func (q *Query) RawQuery(stmt string, args ...interface{}) *Query {
	q.RawSQL = &clause{stmt, args}
	return q
}

// Where will append a where clause to the query. You may use `?` in place of
// arguments.
//
// 	c.Where("id = ?", 1)
// 	q.Where("id in (?)", 1, 2, 3)
func (c *Connection) Where(stmt string, args ...interface{}) *Query {
	q := Q(c)
	return q.Where(stmt, args...)
}

// Where will append a where clause to the query. You may use `?` in place of
// arguments.
//
// 	q.Where("id = ?", 1)
// 	q.Where("id in (?)", 1, 2, 3)
func (q *Query) Where(stmt string, args ...interface{}) *Query {
	if q.RawSQL.Fragment != "" {
		fmt.Println("Warning: Query is setup to use raw SQL")
		return q
	}
	if inRegex.MatchString(stmt) {
		var inq []string
		for i := 0; i < len(args); i++ {
			inq = append(inq, "?")
		}
		qs := fmt.Sprintf("(%s)", strings.Join(inq, ","))
		stmt = strings.Replace(stmt, "(?)", qs, 1)
	}
	q.whereClauses = append(q.whereClauses, clause{stmt, args})
	return q
}

// Order will append an order clause to the query.
//
// 	c.Order("name desc")
func (c *Connection) Order(stmt string) *Query {
	return Q(c).Order(stmt)
}

// Order will append an order clause to the query.
//
// 	q.Order("name desc")
func (q *Query) Order(stmt string) *Query {
	if q.RawSQL.Fragment != "" {
		fmt.Println("Warning: Query is setup to use raw SQL")
		return q
	}
	q.orderClauses = append(q.orderClauses, clause{stmt, []interface{}{}})
	return q
}

// Limit will add a limit clause to the query.
func (c *Connection) Limit(limit int) *Query {
	return Q(c).Limit(limit)
}

// Limit will add a limit clause to the query.
func (q *Query) Limit(limit int) *Query {
	q.limitResults = limit
	return q
}

// Q will create a new "empty" query from the current connection.
func Q(c *Connection) *Query {
	return &Query{
		RawSQL:     &clause{},
		Connection: c,
	}
}

// ToSQL will generate SQL and the appropriate arguments for that SQL
// from the `Model` passed in.
func (q Query) ToSQL(model *Model) (string, []interface{}) {
	sb := q.toSQLBuilder(model)
	return sb.String(), sb.Args()
}

// ToSQLBuilder returns a new `SQLBuilder` that can be used to generate SQL,
// get arguments, and more.
func (q Query) toSQLBuilder(model *Model) *sqlBuilder {
	return newSQLBuilder(q, model)
}

// All retrieves all of the records in the database that match the query.
//
//	c.All(&[]User{})
func (c *Connection) All(models interface{}) error {
	return Q(c).All(models)
}

// All retrieves all of the records in the database that match the query.
//
//	q.Where("name = ?", "mark").All(&[]User{})
func (q *Query) All(models interface{}) error {
	m := &Model{Value: models}
	if err := q.Connection.Dialect.SelectMany(q.Connection.DB, m, *q); err != nil {
		return err
	}
	return nil
}

// Create inserts a new model or slice of models
func (c *Connection) Create(model interface{}) error {
	sm := &Model{Value: model}
	return sm.iterate(func(m *Model) error {
		if err := c.Dialect.Create(c.DB, m); err != nil {
			return err
		}
		return nil
	})
}

// CreateMany inserts a new model or slice of models
func (c *Connection) CreateMany(model interface{}) error {
	sm := &Model{Value: model}
	if err := c.Dialect.CreateMany(c.DB, sm); err != nil {
		return err
	}
	return nil
}

// Destroy deletes a given entry from the database
func (c *Connection) Destroy(model interface{}) error {
	sm := &Model{Value: model}
	return sm.iterate(func(m *Model) error {
		if err := c.Dialect.Destroy(c.DB, m); err != nil {
			return err
		}
		return nil
	})
}

// DestroyMany deletes many entries from a database
func (c *Connection) DestroyMany(models interface{}) error {
	m := &Model{Value: models}
	if err := c.Dialect.DestroyMany(c.DB, m); err != nil {
		return err
	}
	return nil
}

// Update updates a record
func (c *Connection) Update(model interface{}) error {
	sm := &Model{Value: model}
	return sm.iterate(func(m *Model) error {
		var err error
		m.touchUpdatedAt()
		if err = c.Dialect.Update(c.DB, m); err != nil {
			return err
		}
		return nil
	})
}

func (c *Connection) SQLView(model interface{}, format map[string]string) error {
	m := &Model{Value: model}
	if err := c.Dialect.SQLView(c.DB, m, format); err != nil {
		return err
	}
	return nil
}

func (c *Connection) CreateTable(model interface{}) error {
	m := &Model{Value: model}
	if err := c.Dialect.CreateTable(c.DB, m); err != nil {
		return err
	}
	return nil
}
