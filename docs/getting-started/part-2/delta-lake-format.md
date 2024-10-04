---
title: Delta Lake - a better data format
---

## Goal

Up to now we have used CSV with CsvFileDataObject as file format. We will switch to a more modern data format in this step which supports a catalog, compression and even transactions.

## File formats

Smart Data Lake Builder has built in support for many data formats and technologies. 
An important one is storing files on a Hadoop filesystem, supporting standard file formats such as CSV, Json, Avro or Parquet.
In Part 1 we have used CSV through the CsvFileDataObject. CSV files can be easily checked in an editor or Excel, but the format also has many problems, e.g. support of multi-line strings or lack of data type definition.
Often Parquet format is used, as it includes a schema definition and is very space efficient through its columnar compression.

## Catalog

Just storing files on Hadoop filesystem makes it difficult to use them in a SQL engine such as Spark SQL. You need a metadata layer on top which stores table definitions. This is also called a metastore or catalog.
If you start a Spark session, a configuration to connect to an external catalog can be set, or otherwise Spark creates an internal catalog for the session.
We could register our CSV files in this catalog by creating a table via a DDL-statement, including the definition of all columns, a path and the format of our data.
But you could also directly create and write into a table by using Spark Hive tables. 
SDLB supports this by the HiveTableDataObject. It always uses Parquet file format in the background as a best practice, although Hive tables could also be created on top of CSV files.

:::info
Hive is a Metadata layer and SQL engine on top of a Hadoop filesystem. Spark uses the metadata layer of Hive, but implements its own SQL engine.
:::

## Transactions

Hive tables with Parquet format are lacking transactions. This means for example that writing and reading the table at the same time could result in failure or empty results. 
In consequence 
* consecutive jobs need to be synchronized
* it's not recommended having end-user accessing the table while data processing jobs are running
* update and deletes are not supported

There are other options like classical databases which always had a metadata layer, offer transactions but don't integrate easily with Hive metastore and cheap, scalable Hadoop file storage.
Nevertheless, SDLB supports classical databases through the JdbcTableDataObject.
Fortunately there is a new technology called *Open Table Formats* with implementations like Delta Lake (see also [delta.io](https://delta.io/)), Iceberg or Hudi. 
They integrate tables into a Hive metastore, supports transactions and store Parquet files and a transaction log on hadoop filesystems.
SDLB supports provides a DeltaLakeTableDataObject and IcebergTableDataObject.
We are going to use DeltaLakeTableDataObject for our airport and departure data now.

## DeltaLakeTableDataObject

Switching to Delta Lake format is easy with SDLB, just replace `CsvFileDataObject` with `DeltaLakeTableDataObject` and define the table's db and name.
Let's start by changing the existing definitions for `int-airports`, `btl-departures-arrivals-airports` and `btl-distances`.

Update DataObject `int-airports` in airports.conf:
```
  int-airports {
    type = DeltaLakeTableDataObject
    path = "~{id}"
    table = {
      db = "default"
      name = "int_airports"
    }
  }
```

Update DataObjects `btl-departures-arrivals-airports` and `btl-distances` in btl.conf:
```
  btl-departures-arrivals-airports {
    type = DeltaLakeTableDataObject
    path = "~{id}"
    table {
      db = "default"
      name = "btl_departures_arrivals_airports"
    }
  }
  
  btl-distances {
    type = DeltaLakeTableDataObject
    path = "~{id}"
    table {
      db = "default"
      name = "btl_distances"
    }
  }
```

Then create a new, similar data object `int-departures` in departures.conf: 
```
  int-departures {
    type = DeltaLakeTableDataObject
    path = "~{id}"
    table = {
      db = default
      name = int_departures
    }
  }
```

Next, create a new action `prepare-departures` in the `actions` section of departures.conf to fill the new table with the data:
```
  prepare-departures {
    type = CopyAction
    inputId = stg-departures
    outputId = int-departures
    metadata {
      feed = compute
    }
  }
```

Finally, adapt the action definition for `join-departures-airports` in btl.conf:
* change `stg-departures` to `int-departures` in inputIds
* change `stg_departures` to `int_departures` in the first SQLDfsTransformer (watch out, you need to replace the string 4 times)

:::info Explanation
- We changed `int-airports`, `btl-departures-arrivals-airports` and `btl-distances` from CSV to Delta Lake format
- Created an additional table `int-departures`
- Created an action `prepare-departures` to fill the new integration layer table `int-departures`
- Adapted the existing action `join-departures-airports` to use the new table `int-departures`
:::

To run our data pipeline, first delete `data/int-*` and `data/btl-*` - otherwise DeltaLakeTableDataObject will fail because of existing files in different format.
Then you can execute the usual *./startJob.sh* command for `compute` feed:

```
./startJob.sh -c /mnt/config --feed-sel 'compute'
```

Getting an error like `io.smartdatalake.util.webservice.WebserviceException: Read timed out`? Check the list of [Common Problems](../troubleshooting/common-problems) for a workaround.

Getting an error like `DeltaAnalysisException: [DELTA_TABLE_NOT_FOUND] Delta table default.int_departures doesn't exist.`?
This happens when you delete the files of a Delta Table on the filesystem, but the table is still registered in the metastore.
Solution: delete data/metastore_db directory to reset the metastore completely, or issue a `spark.sql("DROP TABLE default.int_departures")` in spark-shell. See below on how to start spark-shell. 

## Reading Delta Lake Format with Spark

Checking our results gets more complicated now - we can't just open delta lake format in a text editor like we used to do for CSV files.
But we can now use SQL to query our results, that is even better. 
We will use a Spark session for this, i.e. by starting a spark-shell.
Alternatively one could use notebooks like Jupyter, Polynote or Databricks for this.
See our [Polynote-lab](https://github.com/smart-data-lake/polynote-lab) for a local solution.

Execute the following command to start a spark-shell. Note that his starts a spark-shell having SDLB and your Apps code in the class-path, using the same data/metastore_db as metastore like ./startJob.sh.

```
./sparkShell.sh
```

List existing tables in schema "default": 
```
spark.catalog.listTables("default").show(false)
```

should show

    +--------------------------------+-------------+---------+-----------+---------+-----------+
    |name                            |catalog      |namespace|description|tableType|isTemporary|
    +--------------------------------+-------------+---------+-----------+---------+-----------+
    |btl_departures_arrivals_airports|spark_catalog|[default]|NULL       |EXTERNAL |false      |
    |btl_distances                   |spark_catalog|[default]|NULL       |EXTERNAL |false      |
    |int_airports                    |spark_catalog|[default]|NULL       |EXTERNAL |false      |
    |int_departures                  |spark_catalog|[default]|NULL       |EXTERNAL |false      |
    +--------------------------------+-------------+---------+-----------+---------+-----------+

List schema and data of table btl_distances:
```
spark.table("default.btl_distances").printSchema
spark.table("default.btl_distances").limit(5).show
```

should look similar to 

    root
    |-- estdepartureairport: string (nullable = true)
    |-- estarrivalairport: string (nullable = true)
    |-- arr_name: string (nullable = true)
    |-- arr_latitude_deg: string (nullable = true)
    |-- arr_longitude_deg: string (nullable = true)
    |-- dep_name: string (nullable = true)
    |-- dep_latitude_deg: string (nullable = true)
    |-- dep_longitude_deg: string (nullable = true)
    |-- distance: double (nullable = true)
    |-- could_be_done_by_rail: boolean (nullable = true)

    +-------------------+-----------------+--------------------+----------------+-----------------+------------+----------------+-----------------+------------------+---------------------+
    |estdepartureairport|estarrivalairport|            arr_name|arr_latitude_deg|arr_longitude_deg|    dep_name|dep_latitude_deg|dep_longitude_deg|          distance|could_be_done_by_rail|
    +-------------------+-----------------+--------------------+----------------+-----------------+------------+----------------+-----------------+------------------+---------------------+
    |               LSZB|             LEAL|Alicante-Elche Mi...|         38.2822|        -0.558156|Bern Airport|       46.912868|         7.498512|1163.0363811380448|                false|
    |               LSZB|             LFLY|   Lyon Bron Airport|       45.727947|         4.943991|Bern Airport|       46.912868|         7.498512|236.29247119523134|                 true|
    |               LSZB|             LFMD|Cannes-Mandelieu ...|       43.547998|         6.955176|Bern Airport|       46.912868|         7.498512|376.56517532159864|                 true|
    |               LSZB|             LFMN|Nice-Côte d'Azur ...|       43.658401|          7.21587|Bern Airport|       46.912868|         7.498512|362.55441725133795|                 true|
    |               LSZB|             LGRP|    Diagoras Airport|       36.405399|        28.086201|Bern Airport|       46.912868|         7.498512| 2061.217367266584|                false|
    +-------------------+-----------------+--------------------+----------------+-----------------+------------+----------------+-----------------+------------------+---------------------+

You can also use SDLB's scala interface to access DataObjects and Actions in the spark-shell. The interface is generated through `./buildJob.sh` and it is important to re-execute buildJob.sh after changes on configurations files before starting the spark-shell.
Then execute the following command in spark-shell to initialize the SDLB interface: 

```
import io.smartdatalake.generated._
import io.smartdatalake.lab.SmartDataLakeBuilderLab
val sdlb = SmartDataLakeBuilderLab[DataObjectCatalog, ActionCatalog](spark,Seq("/mnt/config"), DataObjectCatalog(_, _), ActionCatalog(_, _))
implicit val context = sdlb.context
```

Now you can show or drop DataObjects as follows. Note that DataObjectId is converted from hyphen separated to camelCase style for Java/Scala compatibility.

```
sdlb.dataObjects.intDepartures.printSchema
sdlb.dataObjects.intDepartures.get.limit(5).show
sdlb.dataObjects.intDepartures.dataObject.dropTable
```

You can find a detailed description of SDLB's scala interface [here](../../reference/notebookCatalog)

To automatically initialize SDLB's scala interface on spark-shell startup, uncomment the corresponding code in `shell.scala`.

:::tip Delta Lake tuning
You might have seen that our data pipeline with DeltaTableDataObject runs a Spark stage with 50 tasks several times.
This is delta lake reading its transaction log with Spark. For our data volume, 50 tasks are way too much.
You can reduce the number of snapshot partitions to speed up the execution by setting the following Spark property in your `global.conf` under `global.spark-options`:

    "spark.databricks.delta.snapshotPartitions" = 2

:::

:::tip Hocon file splitting - global.conf
Note that SDLB config files can be split arbitrarily. They will be merged by the Hocon parser.
An SDLB best practice is to use a separate global.conf file for the global configuration.
:::

Your departures/airports/btl.conf should now look like the files ending with `part-2a-solution` in [this directory](https://github.com/smart-data-lake/getting-started/tree/master/config).
Feel free to play around.

In the next step, we are going to take a look at keeping historical data...
