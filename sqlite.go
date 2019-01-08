package goala

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
)

func ConnectSQLLite(file string) (*Connection, error) {
	db, err := sqlx.Open("sqlite3", file+"?parseTime=true")
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(0)
	if err := db.Ping(); err != nil {
		return nil, err
	}
	return &Connection{
		DB:      db,
		Dialect: &sqlite3{},
	}, nil
}

type sqlite3 struct{}

func (s *sqlite3) Name() string {
	return "sqlite3"
}

func (s *sqlite3) TranslateSQL(sql string) string {
	return sql
}

func (s *sqlite3) Create(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreate(db, model), "sqlite3 create")
}

func (s *sqlite3) CreateMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateMany(db, model), "sqlite3 create")
}

func (s *sqlite3) Update(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericUpdate(db, model), "sqlite3 update")
}

func (s *sqlite3) Destroy(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroy(db, model), "sqlite3 destroy")
}

func (s *sqlite3) DestroyMany(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericDestroyMany(db, model), "sqlite3 destroy many")
}

func (s *sqlite3) SelectOne(db *sqlx.DB, model *Model, query Query) error {
	return errors.Wrap(genericSelectOne(db, model, query), "sqlite3 select one")
}

func (s *sqlite3) SelectMany(db *sqlx.DB, models *Model, query Query) error {
	return errors.Wrap(genericSelectMany(db, models, query), "sqlite3 select many")
}

func (s *sqlite3) SQLView(db *sqlx.DB, models *Model, format map[string]string) error {
	return errors.Wrap(genericSQLView(db, models, format), "sqlite3 sql view")
}

func (s *sqlite3) CreateTable(db *sqlx.DB, model *Model) error {
	return errors.Wrap(genericCreateTable(db, model), "sqlite3 create table")
}
