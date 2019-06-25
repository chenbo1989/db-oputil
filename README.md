# db-oputil
This project consists of some experimental utils for database management or visualization. 

## Basic
- DBPool provides a pool of database conections, created by a .ini file
- JDBCBasicQuery a convinient wrapper of Java JDBC API, can do query and execute. 

## gensql
Parse create SQL, generate some new SQLs

## mysqlbinlog2
based on Alibaba canal framework, provides a configuration to accept MySQL binlog INSERT/UPDATE/DELETE and apply to target database.

see main/resource/binlog-consumers.cfg

## transfer
a naive implementation to sync data from this to that. Just select from source table and insert into target table with certain conditions by where clauses.

## rest
provies RESTFul API using Jersey framework.

