package main

import (
	"../hdb"
	"fmt"
)

type testGoAdapterStruct struct {
	A 	int `hdb:"PK" sql:"A"`
}

type testGoAdapterSave struct {
	A 	int `hdb:"PK" sql:"A"`
	B 	int `sql:"B"`
}

type viewStruct struct {
	ProjectInternalID string `hdb:"PK" sql:"ProjectInternalID"`
}

func SelectFromTable() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	orm := hdb.InitializeModel(conn)

	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")

	orm.SetColumnString("A,SUM(B)")

	orm.SetGroupBy("A")
	orm.SetHaving("A = 4")

	sqlstmt := orm.GenerateSQL(true)
	fmt.Println(sqlstmt)

	rows,error := orm.Exec(sqlstmt,"select")

	if error!=nil{
		fmt.Println(error)
	}

	fmt.Println()
	for _,val := range rows{
		fmt.Println(val.GetInt(0))
		fmt.Println(val.GetInt(1))
	}
	fmt.Println()

	conn.Close()
}

func FindAllFromTable() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	orm := hdb.InitializeModel(conn)

	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")

	whereclause1 := testGoAdapterStruct{A : 5}
	whereclause2 := testGoAdapterStruct{A : 9}

	var sliceWhere []interface{}

	sliceWhere = append(sliceWhere,whereclause1,whereclause2)

	results,_ := orm.FindAll(sliceWhere,true)

	fmt.Println()
	fmt.Println(results)
	fmt.Println()

	conn.Close()

}

func SaveInTable() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	orm := hdb.InitializeModel(conn)

	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")

	orm.SetWhereClause("A = 11",true)
	dataToUpdate := testGoAdapterSave{A : 14,B : 15}
	orm.Save(dataToUpdate,true)

	fmt.Println()
	conn.Close()
}

func DeleteFromTable() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	orm := hdb.InitializeModel(conn)

	orm.SetSchema("DEYSUB")
	orm.SetTable("TESTGOADAPTER")

	dataToDelete1 := testGoAdapterStruct{A : 10}
	dataToDelete2 := testGoAdapterStruct{A : 5}

	var dataToDelete []interface{}

	dataToDelete = append(dataToDelete,dataToDelete1,dataToDelete2)

	orm.DeleteAll(dataToDelete,true)

	fmt.Println()

	conn.Close()

}

func SelectFromCalculationView() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	orm := hdb.InitializeModel(conn)

	orm.SetSchema("_SYS_BIC")
	orm.SetView("\"sap.hba.ecc/Project\"")

	view := viewStruct{ProjectInternalID : "'00003486'"}

	var viewSlice []interface{}

	viewSlice = append(viewSlice,view)

	results,_ := orm.FindAll(viewSlice,true)

	outputMap := results[0]

	fmt.Println()
	fmt.Println(outputMap["Project"])
	fmt.Println()

	conn.Close()
}

func CreateStoredProcedure() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)

	hdb.CreateStoredProcedure("checkProc.sql",true)

	fmt.Println()

	conn.Close()
}

func CallStoredProcedure() {

	conn,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	hdb.SetStoredProcedure("DEYSUB","TESTGOADAPTER2")
	conn.Close()

	conn1,_ := hdb.Connect("vs3","DEYSUB","Algo..addict965431",true)
	hdb.CallStoredProcedure("1,8",true)

	fmt.Println()
	conn1.Close()
}

func main(){

	SelectFromTable()
	FindAllFromTable()
	SaveInTable()
	DeleteFromTable()

	SelectFromCalculationView()

	CreateStoredProcedure()
	CallStoredProcedure()

}