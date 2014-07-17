package main

import (
	_"odbc"
	"fmt"
	"hdb"
)

func main() {
	conn, _ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	_ = hdb.InitializeModel(conn,"pspnr","`")
//	stmt, _ := conn.Prepare("select top 1 * from \"SAP_ECC\".\"PROJ\"")
	stmt, _ := conn.Prepare("SELECT \"COLUMN_NAME\" FROM \"SYS\".\"TABLE_COLUMNS\" where \"SCHEMA_NAME\" = 'SAP_ECC' and \"TABLE_NAME\" = 'PROJ'")
	stmt.Execute("i076326")
	rows, _ := stmt.FetchAll()
	for i, row := range rows {
		fmt.Println(row.GetString(0))
		fmt.Println(i, row)
	}
	stmt.Close()
	conn.Close()
}