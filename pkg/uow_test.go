package uow

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"testing"
)

type (
	repo struct {
		tx *sql.Tx
	}
)

func Test_NewUow(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)
	assert.NotNil(t, u)
}

func Test_Register_And_GetRepository(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)

	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	impl := u.(*uow)
	assert.Len(t, impl.repositories, 1)
	if assert.NotNil(t, impl.repositories["repo1"]) {
		a, err := u.GetRepository(context.Background(), "repo1")
		assert.NoError(t, err)
		assert.NotNil(t, a)
		r, ok := a.(*repo)
		assert.True(t, ok)
		assert.NotNil(t, r.tx)
	}
}

func Test_GetRepository_Error(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)

	_, err = u.GetRepository(context.Background(), "repo1")
	assert.EqualError(t, err, "repository not found: repo1")
}

func Test_Unregister(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)

	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	impl := u.(*uow)
	assert.Len(t, impl.repositories, 1)
	assert.NotNil(t, impl.repositories["repo1"])

	u.Unregister("repo1")
	assert.Len(t, impl.repositories, 0)
}

func Test_Do(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	_, err = db.Exec("CREATE TABLE TEST(ID INTEGER PRIMARY KEY)")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)
	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	assert.NoError(t, u.Do(context.Background(), func(uow Uow) error {
		a, e := uow.GetRepository(context.Background(), "repo1")
		assert.NoError(t, e)
		assert.NotNil(t, a)

		r := a.(*repo)
		rt, e := r.tx.Exec("INSERT INTO TEST (ID) VALUES(1)")
		assert.NoError(t, e)

		ar, e := rt.RowsAffected()
		assert.NoError(t, e)
		assert.Equal(t, int64(1), ar)
		return nil
	}))
}

func Test_GetRepository_ErrorOnFactory(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	db.Close() // close to force error on GetRepository
	u := NewUow(db)
	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	_, err = u.GetRepository(context.Background(), "repo1")
	assert.EqualError(t, err, "sql: database is closed")
}

func Test_Do_Error(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)
	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	assert.EqualError(t, u.Do(context.Background(), func(uow Uow) error {
		return fmt.Errorf("do error")
	}), "do error")
}

func Test_Do_CommitError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)
	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	assert.EqualError(t, u.Do(context.Background(), func(u Uow) error {
		impl := u.(*uow)
		impl.tx.Rollback()
		return nil
	}), "rollback error: sql: transaction has already been committed or rolled back due to: sql: transaction has already been committed or rolled back")
}

func Test_Do_RollbackError(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assert.NoError(t, err)
	defer db.Close()
	u := NewUow(db)
	u.Register("repo1", func(tx *sql.Tx) any {
		return &repo{tx}
	})

	assert.EqualError(t, u.Do(context.Background(), func(u Uow) error {
		impl := u.(*uow)
		impl.tx.Rollback()
		return fmt.Errorf("do error")
	}), "rollback error: sql: transaction has already been committed or rolled back due to: do error")
}
