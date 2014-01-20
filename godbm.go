/*
The MIT License (MIT)

Copyright (c) 2014 isaac dawson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

// A simple postgresql database manager/helper.
package godbm

import (
	"database/sql"
	_ "github.com/lib/pq"
	"sync"
)

// UnknownStmtError holds the invalid key which was attempted in a look up.
type UnknownStmtError struct {
	StmtKey string // description of key
}

// Returned when the supplied key for looking up a prepared statement does not exist.
func (e *UnknownStmtError) Error() string {
	return "godbm: error " + e.StmtKey + " was not found"
}

// ConnectionError
type ConnectionError struct{}

// Returned when the supplied key for looking up a prepared statement does not exist.
func (e *ConnectionError) Error() string {
	return "godbm: error not connected to the database"
}

// SqlStore holds a reference to the database, a list of prepared statements
// and a boolean for if we are connected.
type SqlStore struct {
	sync.RWMutex                      // a mutex to synchronize new statements.
	Connected    bool                 // indicates if we are connected or not.
	db           *sql.DB              // the underlying database reference
	queries      map[string]*sql.Stmt // a map of prepared statements referenced by the key
	username     string               // database username
	password     string               // database password
	dbname       string               // database name to connect to
	host         string               // database host
	sslmode      string               // whether we use ssl or not to connect.

}

// New creates a new *SqlStore with the connection properties as arguments.
func New(username, password, dbname, host string, useSsl bool) *SqlStore {
	s := new(SqlStore)
	s.username = username
	s.password = password
	s.host = host
	s.dbname = dbname
	s.sslmode = "disable"
	if useSsl {
		s.sslmode = "enable"
	}
	return s
}

// Connect connects to the database. Returns err on sql.Open error or sets
// our connected state to true.
func (store *SqlStore) Connect() (err error) {
	store.Connected = false
	store.db, err = sql.Open("postgres", "user="+store.username+" password="+store.password+" dbname="+store.dbname+" host="+store.host+" sslmode="+store.sslmode)
	if err != nil {
		return err
	}
	store.Connected = true
	return err
}

// Disconnect iterates through any prepared statements and closes them then calls close
// on the db driver.
func (store *SqlStore) Disconnect() (err error) {
	for _, v := range store.queries {
		v.Close()
	}
	err = store.db.Close()
	store.Connected = false
	return err
}

// Exec creates a new prepared statement, executes and closes. Takes a query string as the first
// parameter and a variable number of arguments to be used in the statement. Closes the statement
// when finished and returns a sql.Result. You should only use this for testing as creating new
// statements every time is non-performant.
func (store *SqlStore) Exec(query string, data ...interface{}) (results sql.Result, err error) {
	if !store.Connected {
		return nil, &ConnectionError{}
	}

	stmt, err := store.PrepareStatement(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.Exec(data...)

}

// Exec creates a new prepared statement, executes and closes. Takes a query string as the first
// parameter and a variable number of arguments to be used in the statement. Closes the statement
// when finished and returns *sql.Rows if any. You should only use this for testing as creating new
// statements every time is non-performant.
func (store *SqlStore) Query(query string, data ...interface{}) (results *sql.Rows, err error) {
	if !store.Connected {
		return nil, &ConnectionError{}
	}

	stmt, err := store.PrepareStatement(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.Query(data...)
}

// PrepareStatement prepares a query and returns the statement to the caller, or error
// if it is invalid.
func (store *SqlStore) PrepareStatement(query string) (stmt *sql.Stmt, err error) {
	if !store.Connected {
		return nil, &ConnectionError{}
	}

	stmt, err = store.db.Prepare(query)
	if err != nil {
		return nil, err
	}
	return stmt, nil
}

// PrepareAdd creates a prepared statement and safely adds it to our map with the provided key.
func (store *SqlStore) PrepareAdd(key, query string) (err error) {
	if !store.Connected {
		return &ConnectionError{}
	}

	stmt, err := store.PrepareStatement(query)
	if err != nil {
		return err
	}
	defer store.Unlock()

	store.Lock()
	if store.queries != nil {
		store.queries[key] = stmt
	} else {
		store.queries = map[string]*sql.Stmt{key: stmt}
	}
	return nil
}

// PrepareDel safely removes a prepared statement from our store provided it exists.
func (store *SqlStore) PrepareDel(key string) (err error) {
	if !store.Connected {
		return &ConnectionError{}
	}

	defer store.Unlock()

	store.Lock()
	stmt, found := store.queries[key]
	if !found {
		return nil
	}
	err = stmt.Close()
	delete(store.queries, key)
	return err
}

// QueryPrepared executes a prepared statement which is looked up by the provided key. If the key was
// not found, an UnknownStmtError is returned. This method takes a variable number of arguments to
// pass to the underlying statement and returns *sql.Rows or an error.
func (store *SqlStore) QueryPrepared(key string, data ...interface{}) (rows *sql.Rows, err error) {
	if !store.Connected {
		return nil, &ConnectionError{}
	}

	store.RLock()
	stmt, found := store.queries[key]
	store.RUnlock()
	if !found {
		return nil, &UnknownStmtError{StmtKey: key}
	}
	return stmt.Query(data...)
}

// ExecPrepared executes a prepared statement which is looked up by the provided key. If the key was
// not found, an UnknownStmtError is returned. This method takes a variable number of arguments to
// pass to the underlying statement and returns sql.Result or an error.
func (store *SqlStore) ExecPrepared(key string, data ...interface{}) (result sql.Result, err error) {
	if !store.Connected {
		return nil, &ConnectionError{}
	}

	store.RLock()
	stmt, found := store.queries[key]
	store.RUnlock()
	if !found {
		return nil, &UnknownStmtError{StmtKey: key}
	}
	return stmt.Exec(data...)
}
