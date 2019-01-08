package goala

import (
	"database/sql"
	"fmt"
	"log"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/estenssoros/goala/nulls"
	interpol "github.com/imkira/go-interpol"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

const (
	StringType      = 0
	IntType         = iota
	FloatType       = iota
	BoolType        = iota
	TimeType        = iota
	UUIDType        = iota
	NullsStringType = iota
	NullsIntType    = iota
	NullsFloatType  = iota
	NullsTimeType   = iota
	NullsBoolType   = iota
)

func IsErrorNoRows(err error) bool {
	return strings.Contains(err.Error(), "no rows in result set")
}

// EscapeString replaces error causing characters in  a string
func EscapeString(sql string) string {
	dest := make([]byte, 0, 2*len(sql))
	var escape byte
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		escape = 0
		switch c {
		case 0: /* Must be escaped for 'mysql' */
			escape = '0'
			break
		case '\n': /* Must be escaped for logs */
			escape = 'n'
			break
		case '\r':
			escape = 'r'
			break
		case '\\':
			escape = '\\'
			break
		case '\'':
			escape = '\''
			break
		case '"': /* Better safe than sorry */
			escape = '"'
			break
		case '\032': /* This gives problems on Win32 */
			escape = 'Z'
		}
		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}
	return string(dest)
}

// StringSlice converts all fields of a struct to a string slice
func (c *Connection) StringSlice(v interface{}) []string {
	return StringSlice(v)
}

func DataTypes(v interface{}) []int {
	fields := reflect.TypeOf(v)
	values := reflect.ValueOf(v)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	dTypes := make([]int, fields.NumField())
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		if dbTag := field.Tag.Get("db"); dbTag == "" {
			continue
		}
		value := values.Field(i)
		switch value.Kind() {
		case reflect.String:
			dTypes[i] = StringType
		case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64:
			dTypes[i] = IntType
		case reflect.Float64, reflect.Float32:
			dTypes[i] = FloatType
		case reflect.Bool:
			dTypes[i] = BoolType
		default:
			switch field.Type {
			case reflect.TypeOf(time.Time{}):
				dTypes[i] = TimeType
			case reflect.TypeOf(uuid.UUID{}):
				dTypes[i] = UUIDType
			case reflect.TypeOf(nulls.Int{}):
				dTypes[i] = NullsIntType
			case reflect.TypeOf(nulls.String{}):
				dTypes[i] = NullsStringType
			case reflect.TypeOf(nulls.Float64{}):
				dTypes[i] = NullsFloatType
			case reflect.TypeOf(nulls.Time{}):
				dTypes[i] = NullsTimeType
			case reflect.TypeOf(nulls.Bool{}):
				dTypes[i] = NullsBoolType
			default:
				panic(fmt.Sprintf("unknown field type: %v", field.Type))
			}
		}
	}
	return dTypes
}

// StringSlice converts all fields of a struct to a string slice
func StringSlice(v interface{}) []string {
	fields := reflect.TypeOf(v)
	values := reflect.ValueOf(v)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	numFields := fields.NumField()
	stringSlice := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		field := fields.Field(i)
		value := values.Field(i)
		switch value.Kind() {
		case reflect.String:
			v := value.String()
			stringSlice[i] = v
		case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64:
			v := value.Int()
			stringSlice[i] = fmt.Sprintf("%d", v)
		case reflect.Float64, reflect.Float32:
			v := value.Float()
			stringSlice[i] = fmt.Sprintf("%f", v)
		case reflect.Bool:
			v := value.Bool()
			stringSlice[i] = fmt.Sprintf("%v", v)
		default:
			switch field.Type {
			case reflect.TypeOf(time.Time{}):
				v := value.Interface().(time.Time)
				stringSlice[i] = v.Format("2006-01-02 15:04:05")
			case reflect.TypeOf(uuid.UUID{}):
				v := value.Interface().(uuid.UUID)
				stringSlice[i] = fmt.Sprintf("%s", v.String())
			case reflect.TypeOf(nulls.Int{}):
				v := value.Interface().(nulls.Int)
				if v.Valid {
					stringSlice[i] = fmt.Sprintf("%d", v.Int)
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.String{}):
				v := value.Interface().(nulls.String)
				if v.Valid {
					stringSlice[i] = fmt.Sprintf("'%s'", v.String)
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.Float64{}):
				v := value.Interface().(nulls.Float64)
				if v.Valid {
					stringSlice[i] = fmt.Sprintf("%f", v.Float64)
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.Time{}):
				v := value.Interface().(nulls.Time)
				if v.Valid {
					stringSlice[i] = v.Time.Format("'2006-01-02 15:04:05'")
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.Bool{}):
				v := value.Interface().(nulls.Bool)
				if v.Valid {
					if v.Bool {
						stringSlice[i] = "1"
					} else {
						stringSlice[i] = "0"
					}
				} else {
					stringSlice[i] = "NULL"
				}
			default:
				panic(fmt.Sprintf("unknown field type: %v", field.Type))
			}
		}
	}
	return stringSlice
}

// StringTuple converts struct to MySQL compatible string tuple
func StringTuple(c interface{}) string {
	fields := reflect.TypeOf(c)
	values := reflect.ValueOf(c)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	numFields := fields.NumField()
	stringSlice := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		field := fields.Field(i)
		value := values.Field(i)
		switch value.Kind() {
		case reflect.String:
			stringSlice[i] = fmt.Sprintf("'%s'", EscapeString(value.String()))
		case reflect.Int, reflect.Int16, reflect.Int8, reflect.Int32, reflect.Int64:
			stringSlice[i] = fmt.Sprintf("%d", value.Int())
		case reflect.Float64, reflect.Float32:
			v := value.Float()
			if math.IsNaN(v) {
				stringSlice[i] = "NULL"
			} else {
				stringSlice[i] = fmt.Sprintf("%f", v)
			}
		case reflect.Bool:
			stringSlice[i] = fmt.Sprintf("%v", value.Bool())
		default:
			switch field.Type {
			case reflect.TypeOf(time.Time{}):
				v := value.Interface().(time.Time)
				stringSlice[i] = v.Format("'2006-01-02 15:04:05'")
			case reflect.TypeOf(uuid.UUID{}):
				v := value.Interface().(uuid.UUID)
				stringSlice[i] = fmt.Sprintf("'%s'", v.String())
			case reflect.TypeOf(nulls.Int{}):
				v := value.Interface().(nulls.Int)
				if v.Valid {
					stringSlice[i] = fmt.Sprintf("%d", v.Int)
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.String{}):
				v := value.Interface().(nulls.String)
				if v.Valid {
					stringSlice[i] = fmt.Sprintf("'%s'", EscapeString(v.String))
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.Float64{}):
				v := value.Interface().(nulls.Float64)
				if v.Valid {
					if math.IsNaN(v.Float64) {
						stringSlice[i] = "NULL"
					} else {
						stringSlice[i] = fmt.Sprintf("%f", v.Float64)
					}
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.Time{}):
				v := value.Interface().(nulls.Time)
				if v.Valid {
					stringSlice[i] = v.Time.Format("'2006-01-02 15:04:05'")
				} else {
					stringSlice[i] = "NULL"
				}
			case reflect.TypeOf(nulls.Bool{}):
				v := value.Interface().(nulls.Bool)
				if v.Valid {
					if v.Bool {
						stringSlice[i] = "1"
					} else {
						stringSlice[i] = "0"
					}
				} else {
					stringSlice[i] = "NULL"
				}
			default:
				panic(fmt.Sprintf("unknown field type: %v", field.Type))
			}
		}
	}
	return fmt.Sprintf("(%s)", strings.Join(stringSlice, ","))
}

func createSchema(v interface{}) (*Schema, error) {
	tableName, err := TableName(v)
	if err != nil {
		return nil, errors.Wrap(err, "create schema")
	}
	schema := &Schema{
		TableName: tableName,
		Columns:   make(map[string]*Column),
		Order:     []string{},
	}
	dTypes := DataTypes(v)
	names := ColumnNames(v)
	if len(dTypes) == 0 {
		return nil, errors.New("no data marked for schema. did you include the db tag?")
	}
	if len(dTypes) != len(names) {
		return nil, errors.New("create schema: dimension: names does not match data types")
	}
	for i, t := range DataTypes(v) {
		schema.AddColumn(names[i], t)
	}
	return schema, nil
}

// ColumnNames creates a slice of names from a struct
func (c *Connection) ColumnNames(v interface{}) []string {
	return ColumnNames(v)
}

// ColumnNames creates a slice of names from a struct
func ColumnNames(v interface{}) []string {
	fields := reflect.TypeOf(v)
	values := reflect.ValueOf(v)
	if values.Kind() == reflect.Ptr {
		values = values.Elem()
		fields = fields.Elem()
	}
	cols := make([]string, values.NumField())
	for i := 0; i < values.NumField(); i++ {
		f := fields.Field(i)
		cols[i] = f.Tag.Get("db")
	}
	return cols
}

// searches for a model field. returns an error if non exists
func getModelField(v reflect.Value, s string) (reflect.Value, error) {
	fbn := v.FieldByName(s)
	log.Println(fbn)
	if !fbn.IsValid() {
		return fbn, errors.Errorf("Model does not have a field named %s", s)
	}
	return fbn, nil
}

// ToSnakeCase conerts to snakecase
func ToSnakeCase(str string) string {
	var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

type table interface {
	TableName() string
}

// InsertStmt creates insert statement from struct tags
func InsertStmt(t interface{}) string {
	m := &Model{Value: t}
	stmt := "INSERT INTO `%s` (%s) VALUES "
	return fmt.Sprintf(stmt, m.TableName(), m.Columns())
}

// SelectStmt generates a select statement from a struct
func SelectStmt(t interface{}) string {
	m := &Model{Value: t}
	stmt := "SELECT %s FROM `%s`"
	return fmt.Sprintf(stmt, m.Columns(), m.TableName())
}

// TruncateStmt return the truncate statement for a table
func TruncateStmt(t interface{}) string {
	m := &Model{Value: t}
	return fmt.Sprintf("TRUNCATE TABLE %s", m.TableName())
}

// Scanner returns an slice of interface to a struct
// rows.Scan(seaspandb.Scanner(&m)...)
func Scanner(u interface{}) []interface{} {
	val := reflect.ValueOf(u).Elem()
	typ := val.Type()
	v := []interface{}{}
	for i := 0; i < val.NumField(); i++ {
		typeField := typ.Field(i)
		if typeField.Tag.Get("db") == "" {
			continue
		}
		valueField := val.Field(i)
		v = append(v, valueField.Addr().Interface())
	}
	return v
}

func ScanRow(rows *sql.Rows, v interface{}) error {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Ptr {
		return errors.New("passed value to ScanRow must be a pointer")
	}
	if err := rows.Scan(Scanner(v)...); err != nil {
		return err
	}
	return nil
}

// CSVHeaders creates a slice of headers from a struct
func CSVHeaders(c interface{}) []string {
	structValue := reflect.ValueOf(c)
	structType := structValue.Type()
	numFields := structValue.NumField()
	cols := make([]string, numFields)
	for i := 0; i < numFields; i++ {
		f := structType.Field(i)
		cols[i] = f.Tag.Get("db")
	}
	return cols
}

// MustFormatMap formats a string from a map or panics
func MustFormatMap(s string, m map[string]string) string {
	s, err := interpol.WithMap(s, m)
	if err != nil {
		panic(err)
	}
	return s
}

// InsertIgnore crafts insert ignore statement fro mstruct tags
func InsertIgnore(t table) string {
	structValue := reflect.ValueOf(t)
	structType := structValue.Type()

	stmt := "INSERT IGNORE INTO `%s` (%s) VALUES "

	numFields := structValue.NumField()
	cols := make([]string, numFields)

	for i := 0; i < numFields; i++ {
		f := structType.Field(i)
		colName := f.Tag.Get("db")
		if colName == "" {
			colName = ToSnakeCase(f.Name)
		}
		cols[i] = fmt.Sprintf("`%s`", colName)
	}
	return fmt.Sprintf(stmt, t.TableName(), strings.Join(cols, ","))
}
