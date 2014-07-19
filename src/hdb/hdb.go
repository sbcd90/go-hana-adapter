package hdb

import (
		"odbc"
		"fmt"
		_"strconv"
		_"strings"
)

type Model struct {
	Db              *odbc.Connection
	SchemaName      string
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
		model = Model{Db: conn,ColumnStr: "*",QuoteIdentifier: "\""}
	}else {
		model = Model{Db: conn,ColumnStr: options[0].(string),QuoteIdentifier: options[1].(string)}
	}
	return
}

func (orm *Model) SetSchema(schemaName string) *Model{
	
	orm.SchemaName = schemaName
	return orm
}

func (orm *Model) SetTable(tableName string) *Model{

	orm.TableName = tableName
	return orm
}

func (orm *Model) SetPrimaryKey(primaryKey string) *Model{

	fmt.Println(orm.PrimaryKey)
	orm.PrimaryKey = primaryKey
	fmt.Println(orm.PrimaryKey)
	return orm
}

func (orm *Model) SetWhereClause(querystring interface{}, onDebug bool,args ...interface{}) *Model{

	switch querystring := querystring.(type) {

		case string : 
					orm.WhereStr = querystring
		case nil :
					if orm.PrimaryKey != "" {
						orm.WhereStr = fmt.Sprintf("%v%v%v IS NOT NULL", orm.QuoteIdentifier,orm.PrimaryKey,orm.QuoteIdentifier)
						orm.ParamIteration++
					}else{
						if onDebug==true{
							fmt.Println("Primary Key not set..Cannot set where clause")
						}
						return orm
					}
		args = append(args, querystring)							
	}
	orm.ParamStr = args
	return orm
}

func (orm *Model) GenerateSQL(onDebug bool) (sqlstmt string){

	if orm.ColumnStr !="" && orm.TableName !="" && orm.SchemaName !=""{
		sqlstmt = fmt.Sprintf("SELECT %v%v%v FROM %v%v%v.%v%v%v",orm.QuoteIdentifier,orm.ColumnStr,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier)
	}else{
		if onDebug{
			fmt.Println("Column String or Schema Name or Table Name is not set")
		}
		return sqlstmt
	}
	if orm.WhereStr != "" {
		sqlstmt = fmt.Sprintf("%v WHERE %v",sqlstmt,orm.WhereStr)
	}
	fmt.Println(sqlstmt)
	return sqlstmt
}