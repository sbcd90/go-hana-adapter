package hdb

import (
		"odbc"
		"fmt"
)

type Model struct {
	Db              *odbc.Connection
	TableName       string
	LimitStr        int
	OffsetStr       int
	WhereStr        string
	ParamStr        []interface{}
	OrderStr        string
	ColumnStr       string
	PrimaryKey      string
	JoinStr         string
	GroupByStr      string
	HavingStr       string
	QuoteIdentifier string
	ParamIdentifier string
	ParamIteration  int
}

var onDebug = false

func Connect(dsn string, username string, password string, onDebug bool) (conn *odbc.Connection, err *odbc.ODBCError) {

	prepareString := "DSN=" + dsn + ";UID=" + username + ";PWD=" + password
	if onDebug {
		fmt.Println(prepareString)
	}
	conn, err = odbc.Connect(prepareString)

	if err != nil {
		if onDebug {
			fmt.Println(err)
		}
		return
	}
	return
}

func InitializeModel(conn *odbc.Connection, options ...interface{}) (model Model) {	

	if len(options) == 0 {
		model = Model{Db: conn,ColumnStr: "*",QuoteIdentifier: "'"}
	}else {
		model = Model{Db: conn,ColumnStr: options[0].(string),QuoteIdentifier: options[1].(string)}
	}
	return
}