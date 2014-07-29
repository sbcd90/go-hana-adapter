go-hana-adapter
===============

SAP HANA adapter for GoLang

Installation
============

- Install the odbc driver for HANA(only available for Windows & Linux)
- Clone the source code from here
- Go into the directory odbc
- execute make.bat or make.bash based on the current OS
- Go into the directory hdb
- execute "go install"

Features
========

- Supports SAP HANA View querying
- Supports Stored Procedure creation, calling, & dropping
- Returns Go maps which are easy to retrieve data from
- Allows developers to easily query using api s like Find,FindAll,Save etc.
- Supports Update,Upsert, & Delete

All examples can be checked in /tests/testOrm.go 

Examples
========

Check the tests directory for the test cases

Travis-CI tests to be provided.