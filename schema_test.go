package goala

import (
	"testing"
	"time"

	"github.com/estenssoros/goala/nulls"
	"github.com/satori/go.uuid"
)

type TestStruct struct {
	ID              uuid.UUID     `json:"id" db:"id_field"`
	TimeField       time.Time     `json:"time" db:"time_field"`
	StringField     string        `json:"string" db:"string_field"`
	FloatField      float64       `json:"float" db:"float_field"`
	IntField        int           `json:"int" db:"int_field"`
	BoolField       bool          `json:"bool" db:"bool_field"`
	NullStringField nulls.String  `json:"null_string" db:"null_string"`
	NullFloatField  nulls.Float64 `json:"null_float" db:"null_float"`
	NullIntField    nulls.Int     `json:"null_int" db:"null_int"`
	NullBoolField   nulls.Bool    `json:"null_bool" db:"null_bool"`
}

func (t TestStruct) TableName() string {
	return `test`
}

func newTestStruct() *TestStruct {
	return &TestStruct{
		ID:              uuid.Must(uuid.NewV4()),
		TimeField:       time.Now(),
		StringField:     "asdf",
		FloatField:      7.0,
		IntField:        7,
		BoolField:       true,
		NullStringField: nulls.NewString("asdf"),
		NullFloatField:  nulls.NewFloat64(7.0),
		NullIntField:    nulls.NewInt(7),
		NullBoolField:   nulls.NewBool(true),
	}
}

func TestCreateSchema(t *testing.T) {
	testStruct := newTestStruct()
	schema, err := createSchema(testStruct)
	if err != nil {
		t.Fatal(err)
	}
	dataTypes := []int{UUIDType, TimeType, StringType, FloatType, IntType, BoolType, NullsStringType, NullsFloatType, NullsIntType, NullsBoolType}
	for i := 0; i < len(dataTypes); i++ {
		name := schema.Order[i]
		c := schema.Columns[name]
		if want, have := dataTypes[i], c.DataType; want != have {
			t.Errorf("have: %v want %v", want, have)
		}
	}
}

func TestCreateSchemaModel(t *testing.T) {
	testStruct := newTestStruct()
	m := &Model{Value: testStruct}
	schema, err := m.CreateSchema()
	if err != nil {
		t.Fatal(err)
	}
	dataTypes := []int{UUIDType, TimeType, StringType, FloatType, IntType, BoolType, NullsStringType, NullsFloatType, NullsIntType, NullsBoolType}
	for i := 0; i < len(dataTypes); i++ {
		name := schema.Order[i]
		c := schema.Columns[name]
		if want, have := dataTypes[i], c.DataType; want != have {
			t.Errorf("have: %v want %v", want, have)
		}
	}
	if _, err := schema.SQL(); err != nil {
		t.Error(err)
	}
}
