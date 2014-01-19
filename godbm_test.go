package godbm

import (
	"fmt"
	"testing"
)

const (
	username = "postgres"
	password = "h0h0h0"
	dbname   = "godbm_test"
	host     = "localhost"
)

func TestConnect(t *testing.T) {
	opts := options()
	dbm := New()
	err := dbm.Connect(opts)
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
}

func TestDisconnect(t *testing.T) {
	opts := options()
	dbm := New()
	err := dbm.Connect(opts)
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}

	defer disconnect(dbm)
}

func TestAddStatements(t *testing.T) {
	opts := options()
	dbm := New()
	err := dbm.Connect(opts)
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}

	dbm.PrepareAdd("test", "select * from user")

	defer disconnect(dbm)
}

//helpers
func disconnect(d *SqlStore) error {
	err = dbm.Disconnect()
	if err != nil {
		t.Fatalf("Error disconnecting from the testdatabase: %v\n", err)
	}
}

func options() *DataStoreOptions {
	opts := new(DataStoreOptions)
	opts.DbType = "postgres"
	opts.ConnectionString = fmt.Sprintf("user=%s password=%s dbname=%s host=%s sslmode=disable", username, password, dbname, host)
	return opts
}
