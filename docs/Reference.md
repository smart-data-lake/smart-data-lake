# Reference
After running your first [examples](GettingStarted.md) this page will give you more details on all available options and the core concepts behind Smart Data Lake.

## Configuration - HOCON
The configuration files are stored in the [HOCON](https://github.com/lightbend/config/blob/master/HOCON.md) file format.
The main sections are global, connections, data objects and actions. 
As a starting point, use the [application.conf](https://github.com/smart-data-lake/sdl-examples/blob/develop/src/main/resources/application.conf) from SDL-examples. 
More details and options are described below. 

### User and Password Variables
Usernames and passwords should not be stored in you configuration files in clear text as these files are often stored directly in the version control system.
They should also not be visible in logfiles after execution.
Instead of having the username and password directly, use the following conventions:

Pattern|Meaning
---|---
CLEAR#pd|The variable will be used literally (cleartext). This is only recommended for test environments.
ENV#pd|The values for this variable will be read from the environment variable called "pd". 
DBSECRET#scope.pd|Used in a Databricks environment, i.e. Microsoft Azure. Expects the variable "pd" to be stored in the given scope.

## Global Options
The global section of the configuration is mainly used to set spark options used by all executions.
A good list with examples can be found in the [application.conf](https://github.com/smart-data-lake/sdl-examples/blob/develop/src/main/resources/application.conf) of sdl-examples.

## Connections
Some Data Objects need a connection, e.g. JdbcTableDataObject, as they need to know how to connect to a database.
Instead of defining the connection information for every data object, you can conveniently define it in one place and just use the reference in the data objects.
The possible parameters depend on the connection type. Please note the section on [usernames and password](#user-and-password-variables).

For a list of all available connections, please consult the [API docs](site/scaladocs/io/smartdatalake/workflow/connection/package.html) directly.

In the package overview, you can also see the parameters available to each type of connection and which parameters are optional.

## Data Objects
For a list of all available data objects, please consult the [API docs](site/scaladocs/io/smartdatalake/workflow/dataobject/package.html) directly.
In the package overview, you can also see the parameters available to each type of data object and which parameters are optional.

Data objects are structured in a hierarchy as many attributes are shared between them, i.e. do Hive tables and transactional tables share common attributes modeled in TableDataObject. 

Here is an overview of all data objects:
![data object hierarchy](images/dataobject_hierarchy.png)


## Actions
For a list of all available actions, please consult the [API docs](site/scaladocs/io/smartdatalake/workflow/action/package.html) directly.

In the package overview, you can also see the parameters available to each type of action and which parameters are optional.

# Command Line
SmartDataLakeBuilder is a java application. To run on a cluster with spark-submit use **DefaultSmartDataLakeBuilder** application.
It can be started with the following command line (details see [YARN](YARN.md))
```bash
spark-submit --master yarn --deploy-mode client --class io.smartdatalake.app.DefaultSmartDataLakeBuilder target/smartdatalake_2.11-1.0.3-jar-with-dependencies.jar [arguments]
```
and takes the following arguments:

```
Usage: DefaultSmartDataLakeBuilder [start|recover] [options]
Command: start [options]
Start a SmartDataLakeBuilder run
  -f, --feed-sel <value>   Regex pattern to select the feed to execute.
  -n, --name <value>       Optional name of the application. If not specified feed-sel is used.
  -c, --config <value>     One or multiple configuration files or directories containing configuration files, separated by comma.
  --partition-values <value>
                           Partition values to process in format <partitionColName>=<partitionValue>[,<partitionValue>,...].
  --multi-partition-values <value>
                           Multi partition values to process in format <partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;(<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...].
  --parallelism <value>    Parallelism for DAG run.

Command: recover [options]
Recover a SmartDataLakeBuilder run
  -n, --name <value>       Name of the application to recover.
  -n, --runId <value>      Optional runId of the application run to recover. If not specified latest is used.

General options:
  --state-path <value>     Path to save run state files.
  --override-jars <value>  Comma separated list of jars for child-first class loader. The jars must be present in classpath.
  --help                   Display the help text.
  --version                Display version information.
```
There exists the following adapted applications versions:
- **LocalSmartDataLakeBuilder**:<br>default for Spark master is `local[*]` and it has additional properties to configure Kerberos authentication. Use this application to run in a local environment (e.g. IntelliJ) without cluster deployment.  
- **DatabricksSmartDataLakeBuilder**:<br>see [MicrosoftAzure](MicrosoftAzure.md)

# Concepts
## DAG
## Metadata
