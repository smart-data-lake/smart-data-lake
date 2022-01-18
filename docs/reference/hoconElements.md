---
id: hoconElements
title: Hocon Elements
---

:::warning
This page is under review and currently not visible in the menu.
:::

## Connections
Some Data Objects need a connection, e.g. JdbcTableDataObject, as they need to know how to connect to a database.
Instead of defining the connection information for every data object, you can conveniently define it in one place and just use the reference in the data objects.
The possible parameters depend on the connection type. Please note the section on [usernames and password](#user-and-password-variables).

For a list of all available connections, please consult the [API docs](https://smartdatalake.ch/docs/site/scaladocs/io/smartdatalake/workflow/connection/index.html) directly.

In the package overview, you can also see the parameters available to each type of connection and which parameters are optional.

## Data Objects
For a list of all available data objects, please consult the [API docs](https://smartdatalake.ch/docs/site/scaladocs/io/smartdatalake/workflow/dataobject/index.html) directly.
In the package overview, you can also see the parameters available to each type of data object and which parameters are optional.

Data objects are structured in a hierarchy as many attributes are shared between them, i.e. do Hive tables and transactional tables share common attributes modeled in TableDataObject.

Here is an overview of all data objects:
![data object hierarchy](../images/dataobject_hierarchy.png)

## Actions
For a list of all available actions, please consult the [API docs](https://smartdatalake.ch/docs/site/scaladocs/io/smartdatalake/workflow/action/index.html) directly.

In the package overview, you can also see the parameters available to each type of action and which parameters are optional.
