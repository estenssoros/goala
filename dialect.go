package goala

import (
	"fmt"
	"reflect"
	"strings"

	interpol "github.com/imkira/go-interpol"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

var emptyUUID = uuid.Nil.String()

type dialect interface {
	Name() string
	TranslateSQL(string) string
	Create(*sqlx.DB, *Model) error
	CreateMany(*sqlx.DB, *Model) error
	Update(*sqlx.DB, *Model) error
	Destroy(*sqlx.DB, *Model) error
	DestroyMany(*sqlx.DB, *Model) error
	SelectOne(*sqlx.DB, *Model, Query) error
	SelectMany(*sqlx.DB, *Model, Query) error
	SQLView(*sqlx.DB, *Model, map[string]string) error
	CreateTable(*sqlx.DB, *Model) error
}

func genericCreate(db *sqlx.DB, model *Model) error {
	model.setID(uuid.Must(uuid.NewV4()))
	model.touchCreatedAt()
	model.touchUpdatedAt()
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", model.TableName(), model.Columns(), model.TokenizedString())
	stmt, err := db.PrepareNamed(query)
	if err != nil {
		return errors.WithStack(err)
	}
	if _, err := stmt.Exec(model.Value); err != nil {
		if err := stmt.Close(); err != nil {
			return errors.WithMessage(err, "failed to close statement")
		}
		return errors.WithStack(err)
	}
	return errors.WithMessage(stmt.Close(), "failed to close statement")
}

func genericCreateMany(db *sqlx.DB, model *Model) error {
	if !model.isSlice() {
		return errors.New("must pass slice")
	}
	values := []string{}
	v := reflect.Indirect(reflect.ValueOf(model.Value))
	for i := 0; i < v.Len(); i++ {
		val := v.Index(i)
		newModel := &Model{Value: val.Addr().Interface()}
		newModel.setID(uuid.Must(uuid.NewV4()))
		newModel.touchCreatedAt()
		newModel.touchUpdatedAt()
		values = append(values, StringTuple(newModel.Value))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", model.TableName(), model.Columns(), strings.Join(values, ","))
	if _, err := db.Exec(query); err != nil {
		return err
	}
	return nil
}

func genericUpdate(db *sqlx.DB, model *Model) error {
	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", model.TableName(), model.UpdateString(), model.whereID())
	res, err := db.NamedExec(stmt, model.Value)
	if err != nil {
		return errors.Wrap(err, "updating record")
	}
	if numRows, _ := res.RowsAffected(); numRows == 0 {
		return errors.New("query updated 0 rows")
	}
	return nil
}

func genericDestroy(db *sqlx.DB, model *Model) error {
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", model.TableName(), model.whereID())
	if err := genericExec(db, stmt); err != nil {
		return errors.Wrap(err, "deleting record")
	}
	return nil
}

func genericDestroyMany(db *sqlx.DB, model *Model) error {
	ids := []string{}
	if !model.isSlice() {
		return errors.New("must supply slice")
	}
	v := reflect.Indirect(reflect.ValueOf(model.Value))
	for i := 0; i < v.Len(); i++ {
		val := v.Index(i)
		newModel := &Model{Value: val.Addr().Interface()}
		fbn, err := newModel.fieldByName("ID")
		if err != nil {
			return err
		}
		id, ok := fbn.Interface().(uuid.UUID)
		if !ok {
			return errors.New("error converting value to uuid")
		}
		ids = append(ids, fmt.Sprintf("'%s'", id))
	}
	stmt := fmt.Sprintf("DELETE FROM %s WHERE id IN (%s)", model.TableName(), strings.Join(ids, ","))
	if _, err := db.Exec(stmt); err != nil {
		return errors.Wrap(err, "deleting records")
	}
	return nil
}

func genericExec(db *sqlx.DB, stmt string) error {
	if _, err := db.Exec(stmt); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func genericSelectOne(db *sqlx.DB, model *Model, query Query) error {
	sql, args := query.ToSQL(model)
	if err := db.Get(model.Value, sql, args...); err != nil {
		return err
	}
	return nil
}

func genericSelectMany(db *sqlx.DB, models *Model, query Query) error {
	sql, args := query.ToSQL(models)
	if err := db.Select(models.Value, sql, args...); err != nil {
		return err
	}
	return nil
}

func genericSQLView(db *sqlx.DB, models *Model, format map[string]string) error {
	var (
		err error
		sql string
	)
	sql, err = models.SQLView()
	if format != nil {
		sql, err = interpol.WithMap(sql, format)
		if err != nil {
			return errors.Wrap(err, "formatting sql")
		}
	}
	if models.isSlice() {
		if err := db.Select(models.Value, sql); err != nil {
			return err
		}
	} else {
		if err := db.Get(models.Value, sql); err != nil {
			return err
		}
	}
	return nil
}

func genericCreateTable(db *sqlx.DB, model *Model) error {
	schema, err := model.CreateSchema()
	if err != nil {
		return errors.Wrap(err, "generic create table")
	}
	sql, err := schema.SQL()
	if err != nil {
		return errors.Wrap(err, "generic create table")
	}
	if _, err := db.Exec(sql); err != nil {
		return err
	}
	return nil
}
