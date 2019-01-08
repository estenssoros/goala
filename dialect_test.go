package goala

import "testing"

func TestCreateTable(t *testing.T) {
	db, err := Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	test := newTestStruct()
	if err := db.CreateTable(test); err != nil {
		t.Fatal(err)
	}
}
