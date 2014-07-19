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
	OrderType		string
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

	model.SchemaName = ""
	model.TableName = ""
	model.LimitStr = 0
	model.OffsetStr = 0
	model.WhereStr = ""
	model.ParamStr = make([]interface{}, 0)
	model.OrderStr = ""
	model.OrderType = "ASC"
	model.ColumnStr = "*"
	model.PrimaryKey = "MANDT"
	model.JoinStr = ""
	model.GroupByStr = ""
	model.HavingStr = ""
	model.ParamIteration = 1

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

	//support composite primary key

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
						orm.WhereStr = fmt.Sprintf("%v IS NOT NULL", orm.PrimaryKey)
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
		sqlstmt = fmt.Sprintf("SELECT %v FROM %v.%v",orm.ColumnStr,orm.SchemaName,orm.TableName)
	}else{
		if onDebug{
			fmt.Println("Column String or Schema Name or Table Name is not set")
		}
		return sqlstmt
	}
	if orm.WhereStr != "" {
		sqlstmt = fmt.Sprintf("%v WHERE %v",sqlstmt,orm.WhereStr)
	}
	if orm.GroupByStr !="" {
		sqlstmt = fmt.Sprintf("%v GROUP BY %v",sqlstmt,orm.GroupByStr)
	}
	if orm.HavingStr != "" {
		if orm.GroupByStr != ""{
			sqlstmt = fmt.Sprintf("%v HAVING %v",sqlstmt,orm.HavingStr)
		}else{
			if onDebug{
				fmt.Println("Group By clause not set")
			}
			return sqlstmt
		}
	}
	if orm.OrderStr !="" {
		sqlstmt = fmt.Sprintf("%v ORDER BY %v %v",sqlstmt,orm.OrderStr,orm.OrderType)
	}
	if orm.OffsetStr > 0 {
		if orm.LimitStr > 0 {
			sqlstmt = fmt.Sprintf("%v LIMIT %v OFFSET %v",sqlstmt,orm.LimitStr,orm.OffsetStr)
		}else{
			if onDebug{
				fmt.Println("Limit not specified...Offset is meaningless")
			}
			return sqlstmt
		}
	}
	if orm.LimitStr > 0 && orm.OffsetStr == 0{
		sqlstmt = fmt.Sprintf("%v LIMIT %v",sqlstmt,orm.LimitStr)
	}
	fmt.Println(sqlstmt)
	return sqlstmt
}

func (orm *Model) SetLimit(start int,size ...int) *Model{

	orm.LimitStr = start
	if len(size) > 0 {
		orm.OffsetStr = size[0]
	}
	return orm
}

func (orm *Model) SetOffset(offset int) *Model{

	orm.OffsetStr = offset
	return orm
}

func (orm *Model) SetOrderBy(order string,ordertype ...string) *Model{
	
	orm.OrderStr = order
	if len(ordertype) > 0 {
		orm.OrderType = ordertype[0]
	}
	return orm
}

func (orm *Model) SetColumnString(columnStr string) *Model{

	orm.ColumnStr = columnStr
	return orm
}

func (orm *Model) SetGroupBy(keys string) *Model{

	orm.GroupByStr = keys
	return orm
}

func (orm *Model) SetHaving(conditions string) *Model{

	orm.HavingStr = conditions
	return orm
}