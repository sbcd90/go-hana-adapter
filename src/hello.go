package main

import (
	_"odbc"
	"fmt"
	"hdb"
)
type Userinfo struct {
    Uid     int `hdb:"PK" sql:"UID" tname:"USER_INFO"`
    Departname  string `sql:"DEPARTNAME"`
}

type sample struct {
	A 	string `hdb:"PK" sql:"a"`
	B 	string `sql:"b"`
}

type SQLModel struct {
	Id int `hdb:"PK" sql:"id"`
}

type User struct {
	SQLModel `sql:",inline"`
	Auth int `sql:"auth"`
}

func main() {
	conn, _ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	orm := hdb.InitializeModel(conn,"PSPNR","\"")
	orm.SetSchema("SAP_ECC")
	orm.SetTable("PRPS a")
//	orm.SetPrimaryKey("MANDT")
	mtest := Userinfo{Uid : 1,Departname : "AI"}
	orm.ScanPK(mtest)
	orm.SetWhereClause("\"A\" = 1",true)
	orm.SetLimit(2)
	orm.SetOffset(4)
	orm.SetOrderBy("MANDT,PSPNR")
	orm.SetColumnString("POSID,a.PSPNR")
	orm.SetGroupBy("MANDT")
	orm.SetHaving("PSPNR = '00000221'")
	orm.Join("INNER","PROJ b","a.PSPHI = b.PSPNR")
	orm.GenerateSQL(true)

	properties := make([]map[string]interface{},0)
	prop1 := make(map[string]interface{}) 
	prop1["A"] = "5"
	prop1["B"] = "6"
	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")
	prop2 := make(map[string]interface{}) 
	prop2["A"] = "3"
	prop2["B"] = "4"
	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")
	properties = append(properties,prop1,prop2)
	orm.InsertBatch(properties)
	prop3 := make(map[string]interface{})
	prop3["A"] = "7"
	prop3["B"] = "8"
	orm.Update(prop3,true)
	sqlmodel := SQLModel{Id : 1}
	ret1,_ := hdb.ScanStructIntoMap(sqlmodel)
	user := User{SQLModel : sqlmodel,Auth : 2}
	ret2,_ := hdb.ScanStructIntoMap(user)
	fmt.Println(ret1["id"])
	fmt.Println(ret2["id"])
	csam := sample{A : "5",B : "6"}
	csam1 := sample{A : "2",B : "3"}
	_ = []sample{csam,csam1}
//	orm.DeleteAll(csam2,true)
//	orm.Delete(csam,true)
	orm.DeleteRow(true)
/*	properties := make(map[string]interface{})
	properties["A"] = "1"
	properties["B"] = "2"
	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")*/
//	stmt, _ := conn.Prepare("select top 1 * from \"SAP_ECC\".\"PROJ\"")
/*	stmt, _ := conn.Prepare("SELECT \"COLUMN_NAME\" FROM \"SYS\".\"TABLE_COLUMNS\" where \"SCHEMA_NAME\" = 'SAP_ECC' and \"TABLE_NAME\" = 'PROJ'")
	stmt.Execute("i076326")
	rows, _ := stmt.FetchAll()
	for i, row := range rows {
		fmt.Println(row.GetString(0))
		fmt.Println(i, row)
	}
	stmt.Close()
	conn.Close()*/
}