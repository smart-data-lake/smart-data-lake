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
Local substitution allows to reuse the id of a configuration object inside its attribute definitions by the special token "~{id}". See the following example:
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

