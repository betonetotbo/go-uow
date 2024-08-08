package uow

import (
	"context"
	"database/sql"
	"fmt"
)

type (
	RepositoryFactory = func(*sql.Tx) any

	DoFunc = func(context.Context, Uow) error

	Uow interface {
		Register(name string, fc RepositoryFactory)
		GetRepository(ctx context.Context, name string) (any, error)
		Do(ctx context.Context, fn DoFunc) error
		Unregister(name string)
	}

	uow struct {
		db           *sql.DB
		tx           *sql.Tx
		repositories map[string]RepositoryFactory
	}
)

func NewUow(db *sql.DB) Uow {
	return &uow{
		db:           db,
		repositories: make(map[string]RepositoryFactory),
	}
}

func (u *uow) Register(name string, fc RepositoryFactory) {
	u.repositories[name] = fc
}

func (u *uow) GetRepository(ctx context.Context, name string) (any, error) {
	err := u.startTx(ctx, false)
	if err != nil {
		return nil, err
	}
	rf, found := u.repositories[name]
	if !found {
		return nil, fmt.Errorf("repository not found: %s", name)
	}
	return rf(u.tx), nil
}

func (u *uow) startTx(ctx context.Context, failIfStarted bool) error {
	if u.tx != nil {
		if failIfStarted {
			return fmt.Errorf("transaction already started")
		}
		return nil
	}
	tx, err := u.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	u.tx = tx
	return nil
}

func (u *uow) Do(ctx context.Context, fn DoFunc) error {
	err := u.startTx(ctx, true)
	if err != nil {
		return err
	}
	err = fn(ctx, u)
	if err != nil {
		if errRb := u.rollback(); errRb != nil {
			return fmt.Errorf("rollback error: %s due to: %s", errRb.Error(), err.Error())
		}
		return err
	}
	return u.commitOrRollback()
}

func (u *uow) commitOrRollback() error {
	err := u.tx.Commit()
	if err != nil {
		if errRb := u.rollback(); errRb != nil {
			return fmt.Errorf("rollback error: %s due to: %s", errRb.Error(), err.Error())
		}
		return err
	}
	u.tx = nil
	return nil
}

func (u *uow) rollback() error {
	if u.tx == nil {
		return fmt.Errorf("no active transaction")
	}
	err := u.tx.Rollback()
	u.tx = nil
	return err
}

func (u *uow) Unregister(name string) {
	delete(u.repositories, name)
}
