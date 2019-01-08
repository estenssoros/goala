package goala

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type Schema struct {
	TableName string
	Columns   map[string]*Column
	Order     []string
}

func (s Schema) String() string {
	ju, _ := json.Marshal(s)
	return string(ju)
}

func (m *Model) CreateSchema() (*Schema, error) {
	return createSchema(m.Value)
}

func (s *Schema) AddColumn(name string, dataType int) error {
	c, err := NewColumn(name, dataType)
	if err != nil {
		return err
	}
	if _, ok := s.Columns[name]; ok {
		return errors.Errorf("add column: column: %s already exists", name)
	}
	s.Columns[name] = c
	s.Order = append(s.Order, name)
	return nil
}

func (s *Schema) GetColumn(name string) (*Column, error) {
	if c, ok := s.Columns[name]; !ok {
		return nil, errors.Errorf("missing column: %s", name)
	} else {
		return c, nil
	}
}
func (s *Schema) Len() int {
	return len(s.Order)
}

func (s *Schema) SQL() (string, error) {
	clauses := make([]string, s.Len())
	for i, name := range s.Order {
		c, err := s.GetColumn(name)
		if err != nil {
			return "", errors.Wrap(err, "schema: sql")
		}
		sql, err := c.SQL()
		if err != nil {
			return "", errors.Wrap(err, "schema: sql")
		}
		clauses[i] = sql
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", s.TableName, strings.Join(clauses, ",")), nil
}

type Column struct {
	Name     string
	DataType int
	Length   int
}

func (c Column) String() string {
	ju, _ := json.Marshal(c)
	return string(ju)
}

func NewColumn(name string, dataType int) (*Column, error) {
	switch dataType {
	case StringType, NullsStringType:
		return &Column{name, dataType, 50}, nil
	case IntType, NullsIntType:
		return &Column{name, dataType, -1}, nil
	case FloatType, NullsFloatType:
		return &Column{name, dataType, -1}, nil
	case BoolType, NullsBoolType:
		return &Column{name, dataType, -1}, nil
	case TimeType, NullsTimeType:
		return &Column{name, dataType, -1}, nil
	case UUIDType:
		return &Column{name, dataType, 36}, nil
	}
	return nil, errors.Errorf("missing datatype: %d", dataType)
}

func (c *Column) SQL() (string, error) {
	switch c.DataType {
	case StringType, NullsStringType:
		return fmt.Sprintf("%s TEXT", c.Name), nil
	case IntType, NullsIntType:
		return fmt.Sprintf("%s INT", c.Name), nil
	case FloatType, NullsFloatType:
		return fmt.Sprintf("%s NUMERIC", c.Name), nil
	case BoolType, NullsBoolType:
		return fmt.Sprintf("%s INT", c.Name), nil
	case TimeType, NullsTimeType:
		return fmt.Sprintf("%s NUMERIC", c.Name), nil
	case UUIDType:
		return fmt.Sprintf("%s TEXT", c.Name), nil
	}
	return "", errors.Errorf("missing datatype: %d", c.DataType)
}
