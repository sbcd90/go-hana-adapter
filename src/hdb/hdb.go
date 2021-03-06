package hdb

import (
		"odbc"
		"fmt"
		"strconv"
		"strings"
		"reflect"
		"errors"
		"io/ioutil"
)

type Model struct {
	Db              *odbc.Connection
	SchemaName      string
	TableName       string
	ViewName 		string
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
var resultsMetadata []*odbc.Row
var systemDSN string
var userName string
var passWord string
var procName string
var connection *odbc.Connection

func Connect(dsn string, username string, password string, onDebug bool) (conn *odbc.Connection, err *odbc.ODBCError) {

	systemDSN = dsn
	userName = username
	passWord = password

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
	connection = conn
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

	tmpconn,_ := Connect(systemDSN,userName,passWord,false)  
	tmporm := InitializeModel(tmpconn)
	if orm.SchemaName!=""{
		resultsMetadata,_ = tmporm.Exec("SELECT \"COLUMN_NAME\" FROM \"SYS\".\"TABLE_COLUMNS\" where \"SCHEMA_NAME\" = '" + orm.SchemaName + "' and \"TABLE_NAME\" = '" + orm.TableName + "'","select")
	}
	tmpconn.Close()
	return orm
}

func (orm *Model) SetView(viewName string) *Model{

	orm.ViewName = viewName

	tmpconn,_ := Connect(systemDSN,userName,passWord,false)  
	tmporm := InitializeModel(tmpconn)
	if orm.SchemaName!=""{
		newViewName := strings.Replace(orm.ViewName,"\"","",-1)
		resultsMetadata,_ = tmporm.Exec("SELECT \"COLUMN_NAME\" FROM \"SYS\".\"VIEW_COLUMNS\" where \"SCHEMA_NAME\" = '" + orm.SchemaName + "' and \"VIEW_NAME\" = '" + newViewName + "'","select")
	}
	tmpconn.Close()
	return orm
}

func SetStoredProcedure(schemaName string,procedureName string) (bool){

	procName = "\"" + schemaName + "\".\"" + procedureName + "\""
	tmpconn,_ := Connect(systemDSN,userName,passWord,false)
	tmporm := InitializeModel(tmpconn)
	resultsMetadata,_ = tmporm.Exec("SELECT \"PARAMETER_NAME\" FROM \"SYS\".\"PROCEDURE_PARAMETERS\" where \"SCHEMA_NAME\" = '" + schemaName + "' and \"PROCEDURE_NAME\" = '" + procedureName + "' and \"PARAMETER_TYPE\" = 'OUT'","select")
	tmpconn.Close()

	return true
}

func (orm *Model) SetPrimaryKey(primaryKey string) *Model{

	//support composite primary key

	orm.PrimaryKey = primaryKey
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

func (orm *Model) Find(output interface{},onDebug bool) (map[string]interface{},error) {

	orm.ScanPK(output)
	var keys []string
	var values []string
	var args string

	results,err := ScanStructIntoMap(output)
	if err!=nil{
		return nil,err
	}
	args = ""

	for key,val := range results{
		keys = append(keys,key)
		values = append(values,ConvertAnyTypeToString(val))
		if args!=""{
			args = fmt.Sprintf("%v AND \"%v\" = %v",args,key,ConvertAnyTypeToString(val))
		}else{
			args = fmt.Sprintf("%v = %v",key,val)
		}
	}

	if orm.ColumnStr=="*"{
		var metadata []string
		for _,rowMeta := range resultsMetadata{
			metadata = append(metadata,rowMeta.GetString(0))
		}
		regexp := fmt.Sprintf("\",\"")
		orm.ColumnStr = fmt.Sprintf("\"%v\"",strings.Join(metadata,regexp))
	}


	orm.WhereStr = args
	orm.SetLimit(1)
	resultsSlice,err := orm.FindMap(onDebug)
	if err!=nil{
		return nil,err
	}

	if len(resultsSlice)==0{
		return nil,errors.New("No records found")
	}else if len(resultsSlice)==1{
		return resultsSlice[0],nil
	}else{
		return nil,errors.New("More than 1 record")
	}

}

func (orm *Model) FindAll(rowsSlicePtr []interface{},onDebug bool) (resultsSlice []map[string]interface{}, err error) {
	
	sliceValue := reflect.Indirect(reflect.ValueOf(rowsSlicePtr))

	if sliceValue.Kind() != reflect.Slice{
		return nil,errors.New("need a pointer to a slice")
	}

	orm.ScanPK(rowsSlicePtr[0])

	var keys []string
	var values []string
	var args string

	args = ""
	for count:=0;count<len(rowsSlicePtr);count++{
		results,err := ScanStructIntoMap(rowsSlicePtr[count])
		if err!=nil{
			return nil,err
		}

		if count==0{
			args = args + "("
		}else{
			args = args + " OR ("
		}

		for key,val := range results{
			keys = append(keys,key)
			values = append(values,ConvertAnyTypeToString(val))
			if !strings.HasSuffix(args,"("){
				args = fmt.Sprintf("%v AND \"%v\" = %v",args,key,ConvertAnyTypeToString(val))
			}else{
				args = fmt.Sprintf("%v \"%v\" = %v",args,key,val)
			}
		}
		args = args + " )"	
	}

	if orm.ColumnStr=="*"{
		var metadata []string
		for _,rowMeta := range resultsMetadata{
			metadata = append(metadata,rowMeta.GetString(0))
		}
		regexp := fmt.Sprintf("\",\"")
		orm.ColumnStr = fmt.Sprintf("\"%v\"",strings.Join(metadata,regexp))
	}

	orm.WhereStr = args

	resultsSlice,err = orm.FindMap(onDebug)

	if err!=nil{
		return nil,err
	}

	return resultsSlice,nil
}

func (orm *Model) FindMap(onDebug bool) (resultsSlice []map[string]interface{}, err error) {

	statement := orm.GenerateSQL(onDebug)
	if onDebug{
		fmt.Println(statement)
	}

	results,erro := orm.Exec(statement,"select")

	var singleRow map[string]interface{}

	//error handling to be done
	for _, row := range results {
		singleRow = make(map[string]interface{})
		count := 0
		for _,rowMeta := range resultsMetadata{
			fieldVal := row.Data[count]
			singleRow[rowMeta.GetString(0)] = (reflect.Indirect(reflect.ValueOf(fieldVal))).Interface()
			count = count + 1
		}
		resultsSlice = append(resultsSlice,singleRow)
	}
	if len(resultsSlice) > 0{
		return resultsSlice,nil
	}else{
		return nil,erro
	}
}

func (orm *Model) GenerateSQL(onDebug bool) (sqlstmt string){

	if orm.ColumnStr !="" && orm.TableName !="" && orm.SchemaName !=""{
		sqlstmt = fmt.Sprintf("SELECT %v FROM %v.%v",orm.ColumnStr,orm.SchemaName,orm.TableName)
	}else if orm.ColumnStr !="" && orm.ViewName !="" && orm.SchemaName !=""{
		sqlstmt = fmt.Sprintf("SELECT %v FROM %v.%v",orm.ColumnStr,orm.SchemaName,orm.ViewName)
	}else{
		if onDebug{
			fmt.Println("Column String or Schema Name or Table Name(or View Name) is not set")
		}
		return sqlstmt
	}
	if orm.JoinStr != "" {
		sqlstmt = fmt.Sprintf("%v %v",sqlstmt,orm.JoinStr)
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

func (orm *Model) ScanPK(output interface{}) *Model{


	if reflect.TypeOf(reflect.Indirect(reflect.ValueOf(output)).Interface()).Kind() == reflect.Slice {
		sliceValue := (reflect.Indirect(reflect.ValueOf(output)).Index(0)).Interface()
		sliceElementType := reflect.Indirect(reflect.ValueOf(sliceValue)).Type()

		for count :=0;count<sliceElementType.NumField();count++{
			bb := sliceElementType.Field(count).Tag
			if bb.Get("hdb") == "PK" || reflect.ValueOf(bb).String() == "PK" {
				orm.PrimaryKey = sliceElementType.Field(count).Name
			}
		}
	}else{
		tt := reflect.TypeOf(reflect.Indirect(reflect.ValueOf(output)).Interface())
		for count :=0;count<tt.NumField();count++{
			bb := tt.Field(count).Tag
			if bb.Get("hdb") == "PK" || reflect.ValueOf(bb).String() == "PK" {
				orm.PrimaryKey = tt.Field(count).Name
			}
		}
	}

	return orm
}

func (orm *Model) Join(join_operator string, tableName string, conditions string) *Model{

	if orm.JoinStr != ""{
		orm.JoinStr = orm.JoinStr + fmt.Sprintf(" %v JOIN %v.%v ON %v",join_operator,orm.SchemaName,tableName,conditions)
	}else{
		orm.JoinStr = fmt.Sprintf("%v JOIN %v.%v ON %v",join_operator,orm.SchemaName,tableName,conditions)
	}

	return orm
}

func (orm *Model) Upsert(properties map[string]interface{},onDebug bool) (int64,error) {

	var keys []string
	var placeholders []string
	var args []interface{}

	for key,val := range properties {
		keys = append(keys,key)
		placeholders = append(placeholders,ConvertAnyTypeToString(val))
		orm.ParamIteration++
		args = append(args,val)
	}

	regexp2 := fmt.Sprintf(", ")

	statement := fmt.Sprintf("UPSERT %v%v%v.%v%v%v VALUES (%v) WHERE %v",orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier,strings.Join(placeholders,regexp2),orm.WhereStr)

	if onDebug{
		fmt.Println(statement)
	}

	_,err := orm.Exec(statement,"insert",args)


	if err!=nil{
		return -1,err
	}else{
		return 0,err
	}
}

func (orm *Model) Insert(properties map[string]interface{},onDebug bool) (int64,error) {

	var keys []string
	var placeholders []string
	var args []interface{}

	for key,val := range properties {
		keys = append(keys,key)
		placeholders = append(placeholders,val.(string))
		orm.ParamIteration++
		args = append(args,val)
	}

	regexp1 := fmt.Sprintf("%v,%v",orm.QuoteIdentifier,orm.QuoteIdentifier)
	regexp2 := fmt.Sprintf(", ")

	statement := fmt.Sprintf("INSERT INTO %v%v%v.%v%v%v (%v%v%v) VALUES (%v)",orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier,orm.QuoteIdentifier,strings.Join(keys,regexp1),orm.QuoteIdentifier,strings.Join(placeholders,regexp2))

	if onDebug{
		fmt.Println(statement)
	}

	_,err := orm.Exec(statement,"insert",args)


	if err!=nil{
		return -1,err
	}else{
		return 0,err
	}
}

func (orm *Model) InsertBatch(rows []map[string]interface{}) ([]int64, error){

	var returnTypes []int64

	tableName := orm.TableName
	if len(rows)<=0{
		return returnTypes,nil
	}

	for count := 0;count<len(rows);count++{
		orm.TableName = tableName
		id,_ := orm.Insert(rows[count],true)
		//fixes to be made here
/*		if err!=nil{
			return returnTypes,err
		}*/
		returnTypes = append(returnTypes,id)
	}
	return returnTypes,nil
}

func (orm *Model) Exec(finalQueryString string, stmtType string, args ...interface{}) ([]*odbc.Row,error) {

	stmt,err := orm.Db.Prepare(finalQueryString)
	if err!=nil{
		fmt.Println(err)
		return nil,err
	}

	output := stmt.Execute()
	if output!=nil{
		fmt.Println(output)
	}

	if stmtType!="insert" && stmtType!="update" && stmtType!="delete" {
		rows,err := stmt.FetchAll()
		if err!=nil{
			return nil,err
		}
		stmt.Close()
		orm.Db.Close()
		return rows,err
	}else{
		return nil,err
	}
}

func (orm *Model) Update(properties map[string]interface{},onDebug bool) (int64, error) {

	var updates []string
	var args []interface{}

	for key,val := range properties {
		updates = append(updates,fmt.Sprintf("%v%v%v = %v",orm.QuoteIdentifier,key,orm.QuoteIdentifier,val))
		args = append(args,val)
		orm.ParamIteration++
	}

	args = append(args,orm.ParamStr...)

	var condition string

	if orm.WhereStr!=""{
		condition = fmt.Sprintf("WHERE %v",orm.WhereStr)
	}else{
		condition = ""
	}

	regexp := fmt.Sprintf(", ")

	statement := fmt.Sprintf("UPDATE %v%v%v.%v%v%v SET %v %v",orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier,strings.Join(updates,regexp),condition)

	if onDebug{
		fmt.Println(statement)
	}

	return -1,nil
}

func (orm *Model) Delete(output interface{},onDebug bool) (int64,error) {

	orm.ScanPK(output)

	results,err := ScanStructIntoMap(output)

	if err!=nil{
		return 0,err
	}

	id := results[strings.ToLower(orm.PrimaryKey)]
	condition := fmt.Sprintf("%v%v%v = %v",orm.QuoteIdentifier,orm.PrimaryKey,orm.QuoteIdentifier,id)
	statement := fmt.Sprintf("DELETE FROM %v%v%v.%v%v%v WHERE %v",orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier,condition)

	if onDebug{
		fmt.Println(statement)
	}

	orm.Exec(statement,"delete")

	//error handling to be done
	return 0,nil
}

func (orm *Model) DeleteAll(rowsSlicePtr interface{},onDebug bool) (int64,error) {

	orm.ScanPK(rowsSlicePtr)

	var ids []string

	val := reflect.Indirect(reflect.ValueOf(rowsSlicePtr))
	if val.Len()==0{
		return 0,nil
	}

	for count:=0;count<val.Len();count++{
		results,err := ScanStructIntoMap(val.Index(count).Interface())
		if err!=nil{
			return 0,err
		}
		id := results[orm.PrimaryKey]
		switch id.(type){
		case string :
						ids = append(ids,id.(string))
		case int,int64,int32 :
						str := strconv.Itoa(id.(int))
						ids = append(ids,str)				
		}
	}

	regexp := fmt.Sprintf("','")
	condition := fmt.Sprintf("%v%v%v IN ('%v')",orm.QuoteIdentifier,orm.PrimaryKey,orm.QuoteIdentifier,strings.Join(ids,regexp))

	statement := fmt.Sprintf("DELETE FROM %v%v%v.%v%v%v WHERE %v",orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier,condition)

	if onDebug{
		fmt.Println(statement)
	}

	orm.Exec(statement,"delete")

	//error handling to be done
	return 0,nil
}

func (orm *Model) DeleteRow(onDebug bool) (int64,error) {

	var condition string
	if orm.WhereStr!="" {
		condition = fmt.Sprintf("WHERE %v",orm.WhereStr)
	}else{
		condition = ""
	}

	statement := fmt.Sprintf("DELETE FROM %v%v%v.%v%v%v %v",orm.QuoteIdentifier,orm.SchemaName,orm.QuoteIdentifier,orm.QuoteIdentifier,orm.TableName,orm.QuoteIdentifier,condition)

	if onDebug{
		fmt.Println(statement)
	}

	orm.Exec(statement,"delete")

	//error handling to be done later
	return 0,nil
}

func (orm *Model) Save(output interface{},onDebug bool) error {

	orm.ScanPK(output)

	results,err := ScanStructIntoMap(output)

	if err==nil{
		orm.Upsert(results,onDebug)
	}

	return nil
}

func CreateStoredProcedure(fileName string,onDebug bool) error{

	bytestring,err := ioutil.ReadFile(fileName)

	if err!=nil{
		return err
	}
	storedproc := string(bytestring)

	_,erro := connection.ExecDirect(storedproc)

	if erro!=nil{
		return erro
	}
	return nil
}

func DropStoredProcedure(onDebug bool) error{

	statement := fmt.Sprintf("DROP PROCEDURE %v",procName)
	if onDebug{
		fmt.Println(statement)
	}

	_,erro := connection.ExecDirect(statement)

	if erro!=nil{
		return erro
	}
	return nil
}

func CallStoredProcedure(paramStr string,onDebug bool) error{

	statement := fmt.Sprintf("CALL %v (%v)",procName,paramStr)
	if onDebug{
		fmt.Println(statement)
	}

	_,erro := connection.ExecDirect(statement)

	if erro!=nil{
		return erro
	}
	return nil
}

func ScanStructIntoMap(obj interface{}) (map[string]interface{},error) {

	dataStruct := reflect.Indirect(reflect.ValueOf(obj))
	if dataStruct.Kind() !=reflect.Struct{
		return nil,errors.New("expected a pointer to a struct")
	}

	dataStructType := dataStruct.Type()

	mapped := make(map[string]interface{})

	for count:=0;count<dataStructType.NumField();count++{
		field := dataStructType.Field(count)
		fieldv := dataStruct.Field(count)

		fieldName := field.Name
		bb := field.Tag
		sqlTag := bb.Get("sql")
		sqlTags := strings.Split(sqlTag,",")
		var mapkey string

		inline := false

		if bb.Get("hdb")=="-" || sqlTag=="-" || reflect.ValueOf(bb).String()=="-"{
			continue
		}else if len(sqlTag) > 0{
			if sqlTags[0]=="-"{
				continue
			}
			mapkey = sqlTags[0]
		}else{
			mapkey = fieldName
		}

		if len(sqlTags) > 1{
			if StringArrayContains("inline",sqlTags[1:]){
				inline = true
			}
		}

		if inline{
			map2,err2 := ScanStructIntoMap(fieldv.Interface())
			if err2!=nil{
				return mapped,err2
			}
			for key,val:= range map2{
				mapped[key] = val
			}
		}else{
			value := dataStruct.FieldByName(fieldName).Interface()
			mapped[mapkey] = value
		}
	}
	return mapped,nil
}

func StringArrayContains(needle string, haystack []string) bool {
	//looping through 1 dim map
	for _, v := range haystack {
		if needle == v {
			return true
		}
	}
	return false
}

func ConvertAnyTypeToString(val interface{}) string {

	var str string
	switch (reflect.Indirect(reflect.ValueOf(val))).Kind(){
		case reflect.Int,reflect.Int8,reflect.Int16,reflect.Int32,reflect.Int64 : 
						str = strconv.FormatInt(reflect.ValueOf(reflect.Indirect(reflect.ValueOf(val)).Interface()).Int(),10)
		case reflect.Uint8,reflect.Uint16,reflect.Uint32,reflect.Uint64 :
						str = strconv.FormatUint(reflect.ValueOf(reflect.Indirect(reflect.ValueOf(val)).Interface()).Uint(),10)
		case reflect.Float32,reflect.Float64 :
						str = strconv.FormatFloat(reflect.ValueOf(reflect.Indirect(reflect.ValueOf(val)).Interface()).Float(),'f',-1,64)
		case reflect.Bool :
						str = strconv.FormatBool(reflect.ValueOf(reflect.Indirect(reflect.ValueOf(val)).Interface()).Bool())	
		case reflect.String :
						str = reflect.ValueOf(reflect.Indirect(reflect.ValueOf(val)).Interface()).String()															
	}

	return str
}