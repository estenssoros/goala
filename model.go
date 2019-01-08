package goala

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/estenssoros/dasorm/sqlite"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Value is the contents of a `Model`.
type Value interface{}

type modelIterable func(*Model) error

// Model wraps the end user interface that is passed in to many functions.
type Model struct {
	Value
	tableName string
}

// ID returns the ID of the Model. All models must have an `ID` field this is
// of type `uuid.UUID`.
func (m *Model) ID() interface{} {
	fbn, err := m.fieldByName("ID")
	if err != nil {
		panic(err)
	}
	return fbn.Interface()
}

// TableNameAble interface allows for the customize table mapping
// between a name and the database. For example the value
// `User{}` will automatically map to "users". Implementing `TableNameAble`
// would allow this to change to be changed to whatever you would like.
type TableNameAble interface {
	TableName() string
}

// TableName returns the corresponding name of the underlying database table
// for a given `Model`. See also `TableNameAble` to change the default name of the table.
func (m *Model) TableName() string {
	if n, ok := m.Value.(TableNameAble); ok {
		return n.TableName()
	}
	t := reflect.TypeOf(m.Value)
	name := m.typeName(t)
	return name
}
func TableName(v interface{}) (string, error) {
	if n, ok := v.(TableNameAble); ok {
		return n.TableName(), nil
	}
	return "", errors.New("missing tablename method on struct")
}

// typeName retrieves the name of an array element
func (m *Model) typeName(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		el := t.Elem()
		if el.Kind() == reflect.Ptr {
			el = el.Elem()
		}
		// validates if the elem of slice or array implements TableNameAble interface.
		tableNameAble := (*TableNameAble)(nil)
		if el.Implements(reflect.TypeOf(tableNameAble).Elem()) {
			v := reflect.New(el)
			out := v.MethodByName("TableName").Call([]reflect.Value{})
			name := out[0].String()
			return name
		}
		return el.Name()
	default:
		return t.Name()
	}
}

// SQLViewAble returns the sql associated with a view for a particular struct
type SQLViewAble interface {
	SQLView() string
}

func (m *Model) SQLView() (string, error) {
	if n, ok := m.Value.(SQLViewAble); ok {
		return n.SQLView(), nil
	}
	return m.sqlView(reflect.TypeOf(m.Value))
}

// typeName retrieves the name of an array element
func (m *Model) sqlView(t reflect.Type) (string, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	el := t.Elem()
	switch t.Kind() {
	case reflect.Slice, reflect.Array:
		if el.Kind() == reflect.Ptr {
			el = el.Elem()
		}
		// validates if the elem of slice or array implements TableNameAble interface.
		sqlViewAble := (*SQLViewAble)(nil)
		if el.Implements(reflect.TypeOf(sqlViewAble).Elem()) {
			v := reflect.New(el)
			out := v.MethodByName("SQLView").Call([]reflect.Value{})
			name := out[0].String()
			return name, nil
		}
		return "", errors.Errorf("%s: model does not implement SQLView", el.Name())
	default:
		return "", errors.Errorf("%s: model does not implement SQLView", el.Name())
	}
}

func (m *Model) fieldByName(s string) (reflect.Value, error) {
	el := reflect.ValueOf(m.Value).Elem()
	fbn := el.FieldByName(s)
	if !fbn.IsValid() {
		return fbn, errors.Errorf("Model does not have a field named %s", s)
	}
	return fbn, nil
}

func (m *Model) setID(i interface{}) {
	fbn, err := m.fieldByName("ID")
	if err == nil {
		if fbn.Interface().(uuid.UUID) == (uuid.UUID{}) {
			fbn.Set(reflect.ValueOf(i))
		}
	}
}

func (m *Model) touchCreatedAt() {
	fbn, err := m.fieldByName("CreatedAt")
	if err == nil {
		switch fbn.Type() {
		case reflect.TypeOf(time.Time{}):
			fbn.Set(reflect.ValueOf(time.Now().UTC()))
		case reflect.TypeOf(sqlite.Time{}):
			fbn.Set(reflect.ValueOf(sqlite.Time(time.Now().UTC())))
		}
	}
}

func (m *Model) touchUpdatedAt() {
	fbn, err := m.fieldByName("UpdatedAt")
	if err == nil {
		switch fbn.Type() {
		case reflect.TypeOf(time.Time{}):
			fbn.Set(reflect.ValueOf(time.Now().UTC()))
		case reflect.TypeOf(sqlite.Time{}):
			fbn.Set(reflect.ValueOf(sqlite.Time(time.Now().UTC())))
		}
	}
}

func (m *Model) whereID() string {
	id := m.ID()
	return fmt.Sprintf("id ='%s'", id)
}

func (m *Model) isSlice() bool {
	v := reflect.Indirect(reflect.ValueOf(m.Value))
	return v.Kind() == reflect.Slice || v.Kind() == reflect.Array
}

// ColumnSlice returns a slice of strings representations of db fields
func (m *Model) ColumnSlice() []string {
	t := reflect.TypeOf(m.Value)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() == reflect.Slice {
		t = t.Elem()
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
	}
	numFields := t.NumField()
	cols := []string{}
	for i := 0; i < numFields; i++ {
		f := t.Field(i)
		colName := f.Tag.Get("db")
		if colName == "" {
			continue
		}
		cols = append(cols, fmt.Sprintf("%s", colName))
	}
	return cols
}

// Columns returns columns as string
func (m *Model) Columns() string {
	return strings.Join(m.ColumnSlice(), ",")
}

// ColumnSliceSafe returns a slice of mysql safe strings representations of db fields
func (m *Model) ColumnSliceSafe() []string {
	cols := m.ColumnSlice()
	for i := 0; i < len(cols); i++ {
		cols[i] = fmt.Sprintf("`%s`", cols[i])
	}
	return cols
}

// ColumnsSafe returns mysql safe columns as string
func (m *Model) ColumnsSafe() string {
	cols := m.ColumnSlice()
	for i := 0; i < len(cols); i++ {
		cols[i] = fmt.Sprintf("`%s`", cols[i])
	}
	return strings.Join(cols, ",")
}

func isin(list []string, test string) bool {
	for _, el := range list {
		if el == test {
			return true
		}
	}
	return false
}

// TokenizedString tokenizes columns
func (m *Model) TokenizedString() string {
	cols := m.ColumnSlice()
	for i := 0; i < len(cols); i++ {
		cols[i] = ":" + cols[i]
	}
	return strings.Join(cols, ", ")
}

// UpdateString returns a tokenized update string for a model
func (m *Model) UpdateString() string {
	cols := m.ColumnSlice()
	out := []string{}
	for i := 0; i < len(cols); i++ {
		switch cols[i] {
		case "id", "created_at":
			continue
		default:
			out = append(out, fmt.Sprintf("%s = :%s", cols[i], cols[i]))
		}
	}
	return strings.Join(out, ", ")
}

func (m *Model) iterate(fn modelIterable) error {
	if m.isSlice() {
		v := reflect.Indirect(reflect.ValueOf(m.Value))
		for i := 0; i < v.Len(); i++ {
			val := v.Index(i)
			newModel := &Model{Value: val.Addr().Interface()}
			if err := fn(newModel); err != nil {
				return err
			}
		}
		return nil
	}
	return fn(m)
}
