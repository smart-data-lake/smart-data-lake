---
title: Keeping historical data
---

## Goal

Data generally can be split into two groups:
- Master data:   
data about objects that evolve over time, e.g. an airport, a person, a product... 
- Transactional data:  
data about events that took place at a certain point in time, e.g. a flight, a payment... 

To keep historical data for both these categories, different strategies are applied:
- **Master data** is most often **historized** - this means tracking the evolution of objects over time by introducing a time dimension. 
  Usually this is modelled with two additional attributes "valid_from" and "valid_to", where "valid_from" is an additional primary key column.
- **Transactional data** is usually **deduplicated**, as only the latest state of a specific event is of interest. If an update for an event occurs, the previous information is discarded (or consolidated in special cases).
  Additional care must be taken to keep all historical events, even if they are no longer present in the source system. Often specific housekeeping rules are applied (e.g. retention period), either for legal or cost saving reasons.

## Requirements

For Historization and Deduplication a data pipeline needs to read the state of the output DataObject, merge it with the new state of the input DataObject and write the result to the output DataObject.
To read and write the same DataObject in the same SDLB Action, this must be a transactional DataObject. 
It means the DataObject must implement the interface TransactionalSparkTableDataObject of SDLB.
Luckily in the previous chapter we already upgraded our data pipeline to use DeltaLakeTableDataObject, which is a TransactionalSparkTableDataObject.

Further, we need a key to identify records for a specific object in our data, so we can build the time dimension or deduplicate records of the same object:
- For airport masterdata (`int_airports`) the attribute "ident" clearly serves this purpose.
- For departure data (`int_departures`) it gets more complicated to identify a flight. To simplify, let's assume we're only interested in one flight per aircraft, departure airport and day.
  The key would then be the attributes `icao24`, `estdepartureairport` and `trunc_date`.

## Historization of airport data

To historize airport master data, we have to adapt our configuration as follows:

Add a primary key to the table definition of `int-airports`:
```
  table {
    db = "default"
    name = "int_airports"
    primaryKey = [ident]
  }
```
Note, that a primary key can be a composite primary key, therefore you need to define an array of columns `[ident]`.

For the action `select-airport-cols`, change its type from `CopyAction` to `HistorizeAction`.  
While you're at it, rename it to `historize-airports` to reflect its new function.
```
  historize-airports {
    type = HistorizeAction
    ...
  }
```

With historization, this table will now get two additional columns called `dl_ts_captured` and `dl_ts_delimited`.
Schema evolution of existing tables will be explained later, so for now, just delete the table and it's data for the DataObject `int-airports` through spark-shell and SDLB's scala interface :
```
sdlb.dataObjects.intAirports.dataObject.dropTable
```
See previous chapter for more details.

Then start Action `historize-airports`. 
You may have seen that the `--feed-sel` parameter of SDLB command line supports more options to select actions to execute (see command line help). 
We will now only execute this single action by changing this parameter to `--feed-sel ids:historize-airports`:

```
./startJob.sh -c /mnt/config --feed-sel ids:historize-airports
```

:::tip JDOFatalDataStoreException
Getting error `javax.jdo.JDOFatalDataStoreException: Unable to open a test connection to the given database` and `Another instance of Derby may have already booted the database /mnt/data/metastore_db`?
This is due to the fact that we are using a file-based Derby database as metastore, which can be only opened by one process at a time.
Solution: close spark-shell (Ctrl-D) before running startJob.sh. 
:::

After successful execution you can check the schema and data of our table in spark-shell. 
It now has a time dimension through the two new columns `dl_ts_captured` and `dl_ts_delimited`.
They form a closed interval, meaning start and end time are inclusive. 
It has millisecond precision, but the timestamp value is set to the current time of our data pipeline run.
The two attributes show the time period in which an object with this combination of attribute values has existed in our data source. 
The sampling rate is given by the frequency that our data pipeline is scheduled.
```
sdlb.dataObjects.intAirports.printSchema
```

```
    root
    |-- ident: string (nullable = true)
    |-- name: string (nullable = true)
    |-- latitude_deg: string (nullable = true)
    |-- longitude_deg: string (nullable = true)
    |-- dl_ts_captured: timestamp (nullable = true)
    |-- dl_ts_delimited: timestamp (nullable = true)
```

If you look at the data, there should be only one record per object for now, as we didn't run our data pipeline with historical data yet.
```
  dataIntAirports.get.orderBy($"ident",$"dl_ts_captured").show
```

```
    +-----+--------------------+------------------+-------------------+--------------------+-------------------+
    |ident|                name|      latitude_deg|      longitude_deg|      dl_ts_captured|    dl_ts_delimited|
    +-----+--------------------+------------------+-------------------+--------------------+-------------------+
    |  00A|   Total Rf Heliport|    40.07080078125| -74.93360137939453|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AA|Aero B Ranch Airport|         38.704022|        -101.473911|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AK|        Lowell Field|         59.947733|        -151.692524|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AL|        Epps Airpark| 34.86479949951172| -86.77030181884766|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AR|Newport Hospital ...|           35.6087|         -91.254898|2021-12-05 13:23:...|9999-12-31 00:00:00|
    ...
```

Let's try to simulate the historization process by loading a historical state of the data and see if any of the airports have changed since then.
For this, drop table `int-airports` again.
Then, delete all files in `data/stg-airport` and copy the historical `result.csv` from the folder `data/stg-airports-fallback` into the folder `data/stg-airports`.

Now start the action `historize-airports` (and only historize-airports) again to do an "initial load".
Remember how you do that? That's right, you can define a single action with `--feed-sel ids:historize-airports`.  
Afterward, start actions `download-airports` and `historize-airports` by using the parameter `--feed-sel 'ids:(download|historize)-airports'` to download fresh data and build up the airport history.

Uff, getting error `AnalysisException: [UNSUPPORTED_OVERWRITE.TABLE] Can't overwrite the target that is also being read from`.
Recent versions of Delta Lake don't like if we overwrite a table "also being read from". Unfortunately this is what HistoryAction does.
It reads the historical data from the table, and adds new records for data changed in the current snapshot.
By default, this happens by overwriting the whole table. This is not optimal from a performance point of view, but there was no other way to change data with Hive tables.
As Delta Lake supports merge statement, we can add `mergeModeEnable = true` to our HistorizeAction configuration as follows:
```
  historize-airports {
    type = HistorizeAction
    inputId = stg-airports
    outputId = int-airports    
    mergeModeEnable = true
    ...
  }
```

:::info Merge Mode
`mergeModeEnable = true` tells Deduplicate/HistorizeAction to merge changed data into the output DataObject, instead of overwriting the whole DataObject.
The output DataObject must implement CanMergeDataFrame interface (also called trait in Scala) for this. 
DeltaLakeTableDataObject will then create a complex SQL-Upsert statement to merge new and changed data into existing output data.
:::

Uff, getting error `'SchemaViolationException: (DataObject~int-airports) Schema does not match schema defined on write:` with `superfluousCols=dl_hash`?
This is due to merge mode needing an additional column `dl_hash` to efficiently check for changed data.
Drop table `int-airports` again, then startJob `--feed-sel ids:historize-airports` and afterward `--feed-sel 'ids:(download|historize)-airports'`.

Now check in spark-shell again, and you'll find several airports that have changed between the initial and the current state:
```
  sdlb.dataObjects.intAirports.get
  .groupBy($"ident").count
  .orderBy($"count".desc)
  .show
```

```
    +-------+-----+
    |  ident|count|
    +-------+-----+
    |RU-4111|    2|
    |   LL33|    2|
    |   73CA|    2|
    |CA-0120|    2|
    |   CDV3|    2|
    ...
```

:::tip Tips for spark-shell
You may have noticed that pasting multi-line text into spark-shell executes every line separately.
This works in the example above, but does not work with every scala code. Also, it doesn't look that nice to see all intermediate results.

Enter `:paste` in spark-shell before pasting multi-line text for a better experience.

Also, keep in mind you can autocomplete commands with the tab key. This allows you for instance to autocomplete the commands of the SDLB Interface.
:::

When checking the details it seems that for many airports the number of significant digits was reduced for the position:
```
  sdlb.dataObjects.intAirports.get
  .where($"ident"==="CDV3")
  .drop("dl_hash")
  .show(false)
```

```
    +-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+
    |ident|name                                             |latitude_deg |longitude_deg |dl_ts_captured            |dl_ts_delimited           |
    +-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+
    |CDV3 |Charlottetown (Queen Elizabeth Hospital) Heliport|46.255493    |-63.098887    |2021-12-05 20:52:58.800645|9999-12-31 00:00:00       |
    |CDV3 |Charlottetown (Queen Elizabeth Hospital) Heliport|46.2554925916|-63.0988866091|2021-12-05 20:40:31.629764|2021-12-05 20:52:58.799645|
    +-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+
```

Values for `dl_ts_capture` and `dl_ts_delimited` respectively were set to the current time of our data pipeline run. 
For an initial load, this should be set to the time of the historical data set. 
Currently, this is not possible in SDLB, but there are plans to implement this, see issue [#427](https://github.com/smart-data-lake/smart-data-lake/issues/427).

If you would try to run `download-airports` and `historize-airports` Action in a fresh environment (data/stg-airports directory removed),
the job would fail with `requirement failed: (DataObject~stg-airports) DataObject schema is undefined.` in prepare phase.
This is bug [#900](https://github.com/smart-data-lake/smart-data-lake/issues/900).
For now lets make it more stable by adding a fixed schema to `stg-airports` DataObject as follows:
```
  stg-airports {
    type = CsvFileDataObject
    path = ${env.basePath}"~{id}"
    schema = "id string, ident string, type string, name string, latitude_deg string, longitude_deg string, elevation_ft string, continent string, iso_country string, iso_region string, municipality string, scheduled_service string, gps_code string, iata_code string, local_code string, home_link string, wikipedia_link string, keywords string"
  }
```

Now let's continue with flight data.

:::tip Spark performance
Spark automatically splits jobs into multiple tasks to distribute to its workers. This is how Spark can process large scale datasets.
The HistorizeAction needs to join all existing data with the new input data and check for changes.  
If Spark joins data, it needs two processing stages and a shuffle in between to do so (you can read more about this in various Spark tutorials).
There is a Spark property we can tune for small datasets to reduce the number of tasks created.
The default value is to create 200 tasks in each shuffle. With our dataset, 2 tasks should be enough already.
You can tune this by setting the following property in global.spark-options of global.conf configuration file:
```
    "spark.sql.shuffle.partitions" = 2
```

:::

## Deduplication of flight data

To deduplicate departure flight data, we have to adapt our configuration as follows:

Add a primary key to the table definition of `int-departures`:
```
  table {
    db = "default"
    name = "int_departures"
    primaryKey = [icao24, estdepartureairport, dt]
  }
```

Change the type of action `prepare-departures` from `CopyAction`, this time to `DeduplicateAction` and rename it to `deduplicate-departures`, again to reflect its new type.
It also needs an additional transformers to calculate the new primary key column `dt` derived from the column `firstseen`, and to make sure input data is unique across the primary key of the output DataObject.
Finally, we need to set `mergeModeEnable = true` and `updateCapturedColumnOnlyWhenChanged = true`.

The `deduplicate-departures` Action definition should then look as follows: 
```
  deduplicate-departures {
    type = DeduplicateAction
    inputId = stg-departures
    outputId = int-departures
    mergeModeEnable = true
    updateCapturedColumnOnlyWhenChanged = true
    transformers = [{
      type = SQLDfTransformer
      code = "select stg_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from stg_departures"
    },{
      type = DeduplicateTransformer
      rankingExpression = "lastSeen desc"
    }]
  }
```

:::tip Effect of updateCapturedColumnOnlyWhenChanged
By default DeduplicateAction updates column dl_captured in the output for every record it receives. To reduce the number of updated records, `updateCapturedColumnOnlyWhenChanged = true` can be set. 
In this case column dl_captured is only updated in the output, when some attribute of the record changed.
:::

Now, delete the table and data of the DataObject `int-departures` in spark-shell, to prepare it for the new columns `dt` and `dl_ts_captured`.
```
sdlb.dataObjects.intDepartures.dataObject.dropTable
```

Then start Action deduplicate-departures:
```
./startJob.sh -c /mnt/config --feed-sel ids:deduplicate-departures
```

After successful execution you can check the schema and data of our table in spark-shell. 
The new column `dl_ts_captured` shows the current time of the data pipeline run when this object first occurred in the input data. 
```
  sdlb.dataObjects.intDepartures.printSchema
```  
prints:
```
  root
  |-- arrivalairportcandidatescount: long (nullable = true)
  |-- callsign: string (nullable = true)
  |-- departureairportcandidatescount: long (nullable = true)
  |-- estarrivalairport: string (nullable = true)
  |-- estarrivalairporthorizdistance: long (nullable = true)
  |-- estarrivalairportvertdistance: long (nullable = true)
  |-- estdepartureairport: string (nullable = true)
  |-- estdepartureairporthorizdistance: long (nullable = true)
  |-- estdepartureairportvertdistance: long (nullable = true)
  |-- firstseen: long (nullable = true)
  |-- icao24: string (nullable = true)
  |-- lastseen: long (nullable = true)
  |-- dt: string (nullable = true)
  |-- dl_ts_captured: timestamp (nullable = true)
```

We can check the work of DeduplicateAction by the following query in spark-shell: 
```
sdlb.dataObjects.intDepartures.get
.groupBy($"icao24", $"estdepartureairport", $"dt")
.count
.orderBy($"count".desc)
.show
```

```
    +------+-------------------+--------+-----+
    |icao24|estdepartureairport|      dt|count|
    +------+-------------------+--------+-----+
    |4b43ab|               LSZB|20210829|    1|
    |4b4b8d|               LSZB|20210829|    1|
    |4b1b13|               LSZB|20210829|    1|
    |4b4445|               LSZB|20210829|    1|
    |4b0f70|               LSZB|20210830|    1|
    |4b1a01|               LSZB|20210829|    1|
    |346603|               LSZB|20210829|    1|
    |4b4442|               LSZB|20210829|    1|
    |4d02d7|               LSZB|20210829|    1|
    |4b43ab|               LSZB|20210830|    1|
    ...
```

Note that DeduplicateAction assumes that input data is already unique across the given primary key. With `mergeModeEnable = true` we even get errors otherwise.
DeduplicateAction doesn't deduplicate your input data by default, because deduplication is costly and data often is already unique.
In our example we have duplicates in the input data set, and added the DeduplicateTransformer to our input data.
Instead of using DeduplicateTransformer, we could also implement our own deduplicate logic using the Scala Spark API with ScalaCodeSparkDfTransformer as follows:

```
    transformers = [{
      type = ScalaCodeSparkDfTransformer
      code = """
        import org.apache.spark.sql.{DataFrame, SparkSession}
        def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
          import session.implicits._
          df.dropDuplicates("icao24", "estdepartureairport", "dt")
        }
        // return as function
        transform _
      """
    }]
```

:::info
Note how we have used a third way of defining transformation logic now:  
In part 1 we first used a SQLDfsTransformer writing SQL code.   
Then for the more complex example of computing distances, we used a ScalaClassSparkDfTransformer pointing to a Scala class.   
Here, we simply include Scala code in our configuration file directly.
:::

For sure DeduplicateAction did not have much work to do, as this was the first data load. 
In order to get different data you would need to adjust the unix timestamp parameters in the URL of DataObject `ext-departures`. 
Feel free to play around.

:::info Scala Code
Scala is a compiled language. The compiler creates bytecode which can be run on a JVM.
Normally compilation takes place before execution. So how does it work with scala code in the configuration as in our deduplication logic above?

With Scala, you can compile code on the fly. This is actually what the Scala Shell/REPL is doing as well. 
The Scala code in the configuration above gets compiled when ScalaCodeSparkDfTransformer is instantiated during startup of SDLB.
:::

Your departures/airports.conf should now look like the files ending with `part-2b-solution` in [this directory](https://github.com/smart-data-lake/getting-started/tree/master/config).

In the next step we are going to configure the pipeline for different environments like development, integration and production.