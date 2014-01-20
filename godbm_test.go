package godbm

import (
	"testing"
)

const (
	username = "postgres"
	password = "testpass"
	dbname   = "godbm_test"
	host     = "127.0.0.1"
)

func TestConnect(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
}

func TestDisconnect(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}

	defer disconnect(t, dbm)
}

func TestAddStatements(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
	defer disconnect(t, dbm)

	dbm.PrepareAdd("test", "select * from user")

}

func TestDelStatements(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
	defer disconnect(t, dbm)

	dbm.PrepareAdd("test", "select * from user")
	if _, err := dbm.ExecPrepared("test"); err != nil {
		t.Fatalf("error executing prepared statement: %v\n", err)
	}

	if err := dbm.PrepareDel("test"); err != nil {
		t.Fatalf("error deleting test statement %v\n", err)
	}

}

func TestExec(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
	defer disconnect(t, dbm)

	if _, err = dbm.Exec("create table if not exists test (val1 varchar(5), val2 varchar(10), val3 int)"); err != nil {
		t.Fatalf("error creating table in TestExec: %v\n", err)
	}
}

func TestQuery(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
	defer disconnect(t, dbm)

	rows, err := dbm.Query("select * from user")
	if err != nil {
		t.Fatalf("error executing querey statement: %v\n", err)
	}
	for rows.Next() {
		var user string
		if err := rows.Scan(&user); err != nil {
			t.Fatalf("error getting result: %v\n", err)
		}
		if user != username {
			t.Fatalf("error returned user doesn't match what we logged in as!")
		}
	}
}

func TestPreparedInsertAndQuery(t *testing.T) {
	dbm := New(username, password, dbname, host, false)
	err := dbm.Connect()
	if err != nil {
		t.Fatalf("Error connecting to the testdatabase: %v\n", err)
	}
	defer disconnect(t, dbm)

	createTestTable(t, dbm)

	err = dbm.PrepareAdd("insert", "insert into test (val1, val2, val3) values ($1, $2, $3)")
	if err != nil {
		t.Fatal(err)
	}

	err = dbm.PrepareAdd("get", "select * from test where val3 = $1")
	if err != nil {
		t.Fatal(err)
	}

	_, err = dbm.ExecPrepared("insert", "boop", "zoop", 3)
	if err != nil {
		t.Fatal(err)
	}

	rows, errGet := dbm.QueryPrepared("get", 3)
	if errGet != nil {
		t.Fatal(errGet)
	}

	for rows.Next() {
		var val1, val2 string
		var val3 int
		if err := rows.Scan(&val1, &val2, &val3); err != nil {
			t.Fatal(err)
		}

		if val1 != "boop" && val2 != "zoop" && val3 != 3 {
			t.Fatalf("Error returned values are not correct, got bacK: %v %v %v\n", val1, val2, val3)
		}
		break
	}

}

//helpers
func createTestTable(t *testing.T, dbm *SqlStore) {
	if _, err := dbm.Exec("create table if not exists test (val1 varchar(5), val2 varchar(10), val3 int)"); err != nil {
		t.Fatalf("error creating table: %v\n", err)
	}
}
func disconnect(t *testing.T, dbm *SqlStore) {
	dbm.Exec("drop table test")

	if err := dbm.Disconnect(); err != nil {
		t.Fatalf("Error disconnecting from the testdatabase: %v\n", err)
	}
}
