# Setup of databases for integration tests

This document describes how the databases were setup to run the integration tests. 
While following the official [debezium (3.1.0) documentation](https://debezium.io/documentation/reference/3.1/connectors/index.html) for all the supported connectors, some additions are made, f.ex. for Oracle database there exists no out-of-the-box image on Dockerhub. 
You have to build it first.


**Important Note:** Not all databases were explicitly tested as part of this integration test setup. 
The tests primarily focused on databases that include `org.antlr` as a transitive dependency. 
Additionally, one database (PostgreSQL), which does not rely on this dependency, was also tested. 
The test suite executed against PostgreSQL is intended to serve as a baseline and its successful execution suggests that the underlying data object and configurations are likely compatible with other databases as well. 


## MySQL
Server can easily be setup with an oneliner as docker / podman command:
```shell
 podman run -e MYSQL_ROOT_PASSWORD=mysql_demo --name mysql -p 3306:3306 mysql:8.0.39 --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --gtid-mode=ON --enforce-gtid-consistency
```

Then run following ddl (one after another):
```sql
CREATE DATABASE `demo` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;
```
```sql
using demo;
```
```sql
CREATE TABLE `test` (
  `id` int NOT NULL AUTO_INCREMENT,
  `value` varchar(100) DEFAULT NULL,
  `timestampCol` timestamp NULL DEFAULT NULL,
  `decimalCol` decimal(6,3) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
```
```sql
CREATE TABLE `test2` (
  `id` int NOT NULL AUTO_INCREMENT,
  `value` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

```