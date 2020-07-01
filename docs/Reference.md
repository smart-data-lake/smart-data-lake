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

### Local substitution
Local substitution allows to reuse the id of a configuration object inside its attribute definitions by the special token "~{id}". See the following example:
```
dataObjects [
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
Usage: DefaultSmartDataLakeBuilder [options]
  -f, --feed-sel <value>   Regex pattern to select the feed to execute.
  -n, --name <value>       Optional name of the application. If not specified feed-sel is used.
  -c, --config <value>     One or multiple configuration files or directories containing configuration files, separated by comma.
  --partition-values <value>
                           Partition values to process in format <partitionColName>=<partitionValue>[,<partitionValue>,...].
  --multi-partition-values <value>
                           Multi partition values to process in format <partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;(<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...].
  --parallelism <value>    Parallelism for DAG run.
  --state-path <value>     Path to save run state files. Must be set to enable recovery in case of failures.
  --override-jars <value>  Comma separated list of jars for child-first class loader. The jars must be present in classpath.
  --help                   Display the help text.
  --version                Display version information.
```
There exists the following adapted applications versions:
- **LocalSmartDataLakeBuilder**:<br>default for Spark master is `local[*]` and it has additional properties to configure Kerberos authentication. Use this application to run in a local environment (e.g. IntelliJ) without cluster deployment.  
- **DatabricksSmartDataLakeBuilder**:<br>see [MicrosoftAzure](MicrosoftAzure.md)

# Concepts

## DAG

### Execution phases - early validation
Execution of a SmartDataLakeBuilder run is designed with "early validation" in mind. This means it tries to fail as early as possible if something is wrong. 
Execution therefore involves the following phases.
1. Parse configuration: configuration is parsed and validated 
2. DAG prepare: Preconditions are validated. This includes testing Connections and DataObject structures which must exists. 
3. DAG init: Lineage of Actions according to the DAG is created and validated. For Spark Actions this involves the validation of the DataFrame lineage. A column which doesn't exist but is referenced in a later Action will fail the execution.    
4. DAG exec: Execution of Actions, data is effectively (and only) transferred during this phase.

## Execution modes
Execution modes select the data to be processed. By default, if you start SmartDataLakeBuilder, there is no filter applied. This means every Action reads all data from its input DataObjects. 

### Fixed partition values filter
You can apply a filter manually by specifying parameter --partition-values or --multi-partition-values on the command line. The partition values specified are passed to all start-Actions of a DAG and filtered for every input DataObject by its defined partition columns.
On execution every Action takes the partition values of the input and filters them again for every output DataObject by its defined partition columns, which serve again as partition values for the input of the next Action. 
Note that during execution of the dag, no new partition values are added, they are only filtered.

### Dynamic partition values filter - PartitionDiffMode
Alternatively you can let SmartDataLakeBuilder find missing partitions and set partition values automatically by specifying execution mode PartitionDiffMode.

Often this should only happen on the start-Actions of a DAG, so it's clear what partitions are processed through a specific DAG run.
But its also possible to let every Action in the DAG decide again dynamically, what partitions are missing and should be processed.
You can configure this by the following attributes of an Action:
- initExecutionMode: define the execution mode if this Action is a start-Actions of the DAG
- executionMode: define the execution mode independently of its position is the DAG.
If both attributes are set, initExecutionMode overrides executionMode if the Action is a a start-Action of the DAG.

### Dynamic partition values filter - PartitionDiffMode
Alternatively you can let SmartDataLakeBuilder find missing partitions and set partition values automatically by specifying execution mode PartitionDiffMode.

Often this should only happen on the start-Actions of a DAG, so it's clear what partitions are processed through a specific DAG run.
But its also possible to let every Action in the DAG decide again dynamically, what partitions are missing and should be processed.
You can configure this by the following attributes of an Action:
- initExecutionMode: define the execution mode if this Action is a start-Actions of the DAG
- executionMode: define the execution mode independently of its position in the DAG.
If both attributes are set, initExecutionMode overrides executionMode if the Action is a a start-Action of the DAG.

### Incremental load - SparkStreamingOnceMode
Some DataObjects are not partitioned, but nevertheless you dont want to read all data from the input on every run. You want to load it incrementally.
This can be accomplished by specifying execution mode SparkStreamingOnceMode. Under the hood it uses "Spark Structured Streaming" and triggers a single microbatch (Trigger.Once).
"Spark Structured Streaming" helps keeping state information about processed data. It needs a checkpointLocation configured which can be given as parameter to SparkStreamingOnceMode.

Note that "Spark Structured Streaming" needs an input DataObject supporting the creation of streaming DataFrames. 
For the time only the input sources delivered with Spark Streaming are supported. 
This is KafkaTopicDataObject and all SparkFileDataObjects, see also [Spark StructuredStreaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#creating-streaming-dataframes-and-streaming-datasets).

### Incremental Load - DeltaMode
to be implemented  
