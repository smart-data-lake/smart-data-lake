---
id: hoconVariables
title: Hocon Variables
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Variables
Variables can be used to serve various goals. On the one hand, we want to prevent repeating specifications, keep specifications flexible, but also want to be able to change settings at call time. 

Here are some examples listed. 

## Local Substitution
Local substitution allows to reuse the id of a configuration object inside its attribute definitions by the special token "~\{id}". See the following example:
```
dataObjects {
  dataXY {
    type = HiveTableDataObject
    path = "/data/~{id}"
    table {
      db = "default"
      name = "~{id}"
    }
  }
}
```
Note: local substitution only works in a fixed set of attributes defined in Environment.configPathsForLocalSubstitution.
These are `path`, `table.name`, `createSql`, `preReadSql`, `postReadSql`, `preWriteSql`, `postWriteSql` (also in their dash-separated spelling, e.g. `create-sql`)
and `executionMode.checkpointLocation`.

### Modifiers
The substituted value can be transformed by appending one or more modifiers separated by a pipe, e.g. `~{id|snake}`.
Modifiers can be chained and are applied from left to right, e.g. `~{id|noPrefix|snake}`.

The following modifiers are supported:
- `snake`: converts the value to snake_case. It inserts an underscore at camelCase boundaries (including acronyms), replaces dashes with underscores and lowercases the result.
  Examples: `int-airports` → `int_airports`, `intAirports` → `int_airports`, `my-HTTPServerData` → `my_http_server_data`.
- `noPrefix`: removes the first part of the value. The earliest dash (`-`), underscore (`_`) or camelCase boundary determines the prefix to remove.
  A dash or underscore separator is dropped as well, while for a camelCase boundary the uppercase letter starting the remainder is kept.
  A value without separator is left unchanged.
  Examples: `int-airports` → `airports`, `int_airports` → `airports`, `intAirports` → `Airports`, `abc` → `abc`.

Using an unknown modifier fails with a configuration error.

A typical use case is to derive table names from DataObject ids, which often carry a layer prefix and use dashes that are not allowed in table names:
```
dataObjects {
  int-airportData {
    type = DeltaLakeTableDataObject
    path = "~{id}"                         # -> int-airportData
    table {
      db = "default"
      name = "~{id|noPrefix|snake}"         # -> airport_data
    }
  }
}
```
Modified and plain tokens can be mixed in the same value, e.g. `"~{id|snake}_~{id}"` resolves to `int_airport_data_int-airportData`.

Furthermore, you can reuse specified variables, e.g.
```
devprofile {
  hostname: "jdbc:derby://metastore"
}

global {
  spark-options {
    "spark.hadoop.javax.jdo.option.ConnectionURL" = ${devprofile.hostname}":1527/db"
```

## Environment variables
In Hocon, environment variables can be used. Further we distinguish if the variables are required or optional. 

As an example, requiring a DB password, due to the absence of an elaborated secret store:
```
  "spark.hadoop.javax.jdo.option.ConnectionPassword" = ${METASTOREPW}
```

As another example the environment variable `CONNTIMEOUT` is used to overwrite the value of `connectionTimeoutMs` if it exists:
```
    connectionTimeoutMs = 200000
    connectionTimeoutMs = ${?CONNTIMEOUT}
```

