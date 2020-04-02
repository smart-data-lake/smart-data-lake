# Features

The following is a list of implemented and planned (Future) features of Smart Data Lake Builder.

##### Filebased metadata
* Easily versionable with a VCS for DevOps
* Flexiblestructure by spliting over multiple files and subdirectories
* Easily generateable from third party metadata (e.g. source system table catalog) to automate transformation of huge number of DataObjects

##### Support for complex workflows
* Split, join, parallel execution, multiple start- & end-nodes possible
* Future: Recovery of failed runs

##### Execution Engines
* Spark (DataFrames)
* File (Input&OutputStream)
* Future: Kafka Streams, Flink, …

##### Connectivity
* Spark: diverse connectors (HadoopFS, Hive, DeltaLake, JDBC, Kafka, Splunk, Webservice, JMS) and formats (CSV, JSON, XML, Avro, Parquet, Excel, Access …)
* File: SFTP, Local
* Easily extendable through implementing predefined scala traits

##### Generic Transformations
* Spark based: Copy, Historization, Deduplication
* File based: FileTransfer
* Easily extendable through implementing predefined scala traits

##### Customizable Transformations
* Spark Transformations: 
  * Languages: SQL, Scala (Class, compile from config), Future: Python
  * Many input DataFrames to many outputs DataFrames (but only one output recommended normally, in order to define dependencies as detailed as possible (lineage)
* File Transformations: 
  * Language: Scala
  * Only one to one (one Input Stream to one OutputStream)

##### Early Validation
* Execution in 3 phases before execution
  * Load Config: validate configuration
  * Prepare: validate connections
  * Init: validate Spark DataFrame Lineage (missing columns in transformations of later actions will stop the execution)

##### Execution Modes
* Process all data
* Partition parameters: give partition values to process for start nodes as parameter
* Init Partition Diff: search missing partitions on start nodes and use as parameter
* Future: Partition Diff (every action individually), Incremental (get last processed timestamp from target) , Continous (Streaming)

##### Schema Evolution
* Automatic evolution of data schemas (new column, removed column, changed datatype)
* Support for changes in complex datatypes (e.g. new column in array of struct)

##### Metrics
* Number of rows written per DataObject
* Execution duration per Action

##### Data Catalog
* Report all DataObjects attributes (incl. foreign keys if defined) for visualisation of data catalog in BI tool
* Metadata support for categorizing Actions and DataObjects
* Custom metadata attributes

##### Lineage
* Report all dependencies between DataObjects for visualisation of lineage in BI tool

##### Data Quality
* Metadata support for primary & foreign keys
* Check & report primary key violations by executing primary key checker action
* Future: Metadata support for arbitrary data quality checks
* Future: Report data quality (foreign key matching & arbitrary data quality checks) by executing data quality reporter action
