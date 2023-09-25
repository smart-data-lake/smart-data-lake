---
title: Keeping historical data
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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
To read and write the same DataObject in the same SDL Action, this must be a transactional DataObject. 
It means the DataObject must implement the interface TransactionalSparkTableDataObject of SDL.
Luckily in the previous chapter we already upgraded our data pipeline to use DeltaLakeTableDataObject, which is a TransactionalSparkTableDataObject.

Further, we need a key to identify records for a specific object in our data, so we can build the time dimension or deduplicate records of the same object:
- For airport masterdata (`int_airports`) the attribute "ident" clearly serves this purpose.
- For departure data (`int_departures`) it gets more complicated to identify a flight. To simplify, let's assume we're only interested in one flight per aircraft, departure airport and day.
  The key would then be the attributes `icao24`, `estdepartureairport` and `trunc_date`.

## Historization of airport data

To historize airport master data, we have to adapt our configuration as follows:

Add a primary key to the table definition of `int-airports`:

    table {
      db = "default"
      name = "int_airports"
      primaryKey = [ident]
    }

Note, that a primary key can be a composite primary key, therefore you need to define an array of columns `[ident]`.

For the action `select-airport-cols`, change it's type from `CopyAction` to `HistorizeAction`.  
While you're at it, rename it to `historize-airports` to reflect it's new function.

    historize-airports {
      type = HistorizeAction
      ...
    }

With historization, this table will now get two additional columns called `dl_ts_captured` and `dl_ts_delimited`.
Schema evolution of existing tables will be explained later, so for now, just delete the table and it's data for the DataObject `int-airports` through Polynote.
To access DataObjects from Polynote you need to first read SDL configuration into a registry, see Notebook _SelectingData_ chapter _Select data by using DataObjects configured in SmartDataLake_:

    val dataIntAirports = registry.get[DeltaLakeTableDataObject]("int-airports")
    dataIntAirports.dropTable

:::caution
Depending on your system setup, it's possible that Polynote is not allowed to drop the data of your table. 
If you receive strange errors about dl_ts_captured and dl_ts_delimited not being found, please delete the folder data/int-airports/ manually.
:::

Then start Action `historize-airports`. 
You may have seen that the `--feed-sel` parameter of SDL command line supports more options to select actions to execute (see command line help). 
We will now only execute this single action by changing this parameter to `--feed-sel ids:historize-airports`:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network part2_default sdl-spark:latest -c /mnt/config --feed-sel ids:historize-airports
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --hostname=localhost --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest -c /mnt/config --feed-sel ids:historize-airports
```

</TabItem>
</Tabs>

After successful execution you can check the schema and data of our table in Polynote. 
It now has a time dimension through the two new columns `dl_ts_captured` and `dl_ts_delimited`.
They form a closed interval, meaning start and end time are inclusive. 
It has millisecond precision, but the timestamp value is set to the current time of our data pipeline run.
The two attributes show the time period in which an object with this combination of attribute values has existed in our data source. 
The sampling rate is given by the frequency that our data pipeline is scheduled.

    dataIntAirports.getSparkDataFrame().printSchema

Output:

    root
    |-- ident: string (nullable = true)
    |-- name: string (nullable = true)
    |-- latitude_deg: string (nullable = true)
    |-- longitude_deg: string (nullable = true)
    |-- dl_ts_captured: timestamp (nullable = true)
    |-- dl_ts_delimited: timestamp (nullable = true)

If you look at the data, there should be only one record per object for now, as we didn't run our data pipeline with historical data yet.

    dataIntAirports.getSparkDataFrame().orderBy($"ident",$"dl_ts_captured").show

    +-----+--------------------+------------------+-------------------+--------------------+-------------------+
    |ident|                name|      latitude_deg|      longitude_deg|      dl_ts_captured|    dl_ts_delimited|
    +-----+--------------------+------------------+-------------------+--------------------+-------------------+
    |  00A|   Total Rf Heliport|    40.07080078125| -74.93360137939453|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AA|Aero B Ranch Airport|         38.704022|        -101.473911|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AK|        Lowell Field|         59.947733|        -151.692524|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AL|        Epps Airpark| 34.86479949951172| -86.77030181884766|2021-12-05 13:23:...|9999-12-31 00:00:00|
    | 00AR|Newport Hospital ...|           35.6087|         -91.254898|2021-12-05 13:23:...|9999-12-31 00:00:00|
    ...

Let's try to simulate the historization process by loading a historical state of the data and see if any of the airports have changed since then.
For this, drop table `int-airports` again.
Then, delete all files in `data/stg-airport` and copy the historical `result.csv` from the folder `data-fallback-download/stg-airport` into the folder `data/stg-aiport`.

Now start the action `historize-airports` (and only historize-airports) again to do an "initial load".
Remember how you do that? That's right, you can define a single action with `--feed-sel ids:historize-airports`.  
Afterwards, start actions `download-airports` and `historize-airports` by using the parameter `--feed-sel 'ids:(download|historize)-airports'` to download fresh data and build up the airport history.

Now check in Polynote again and you'll find several airports that have changed between the intitial and the current state:

    dataIntAirports.getSparkDataFrame()
    .groupBy($"ident").count
    .orderBy($"count".desc)
    .show

    +-------+-----+
    |  ident|count|
    +-------+-----+
    |RU-4111|    2|
    |   LL33|    2|
    |   73CA|    2|
    |CA-0120|    2|
    |   CDV3|    2|
    ...

When checking the details it seems that for many airports the number of significant digits was reduced for the position:

    dataIntAirports.getSparkDataFrame()
    .where($"ident"==="CDV3")
    .show(false)
    
    +-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+
    |ident|name                                             |latitude_deg |longitude_deg |dl_ts_captured            |dl_ts_delimited           |
    +-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+
    |CDV3 |Charlottetown (Queen Elizabeth Hospital) Heliport|46.255493    |-63.098887    |2021-12-05 20:52:58.800645|9999-12-31 00:00:00       |
    |CDV3 |Charlottetown (Queen Elizabeth Hospital) Heliport|46.2554925916|-63.0988866091|2021-12-05 20:40:31.629764|2021-12-05 20:52:58.799645|
    +-----+-------------------------------------------------+-------------+--------------+--------------------------+--------------------------+

Values for `dl_ts_capture` and `dl_ts_delimited` respectively were set to the current time of our data pipeline run. 
For an initial load, this should be set to the time of the historical data set. 
Currently, this is not possible in SDL, but there are plans to implement this, see issue [#427](https://github.com/smart-data-lake/smart-data-lake/issues/427).

Now let's continue with flight data.

:::tip Spark performance
Maybe you're under the impression that HistorizeAction runs quite long for a small amount of data. 
And you're right about that:   
On one side this is because in the background, it joins all existing data with the new input data and checks for changes.  
On the other side there is a Spark property we should tune for small datasets. 
If Spark joins data, it needs two processing stages and a shuffle in between to do so (you can read more about this in various Spark tutorials).
The default value is to create 200 tasks in each shuffle. With our dataset, 2 tasks would be enough already.
You can tune this by setting the following property in global.spark-options of your application.conf:

    "spark.sql.shuffle.partitions" = 2

Also, the algorithm to detect and merge changes can be optimized by using Delta formats merge capabilities. This will be covered in part three of this tutorial. 
:::

## Deduplication of flight data

To deduplicate departure flight data, we have to adapt our configuration as follows:

Add a primary key to the table definition of `int-departures`:

    table {
      db = "default"
      name = "int_departures"
      primaryKey = [icao24, estdepartureairport, dt]
    }

Change the type of action `prepare-departures` from `CopyAction`, this time to `DeduplicateAction` and rename it to `deduplicate-departures`, again to reflect its new type.
It also needs an additional transformer to calculate the new primary key column `dt` derived from the column `firstseen`.
So make sure to add these lines too: 

    deduplicate-departures {
      type = DeduplicateAction
      ...
      transformers = [{
        type = SQLDfTransformer
        code = "select stg_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from stg_departures"
      }]
      ...
    }

Now, delete the table and data of the DataObject `int-departures` in Polynote, to prepare it for the new columns `dt` and `dl_ts_captured`.

    val dataIntDepartures = registry.get[DeltaLakeTableDataObject]("int-departures")
    dataIntDepartures.dropTable

Then start Action deduplicate-departures:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network part2_default sdl-spark:latest -c /mnt/config --feed-sel ids:deduplicate-departures
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm --hostname=localhost -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest -c /mnt/config --feed-sel ids:deduplicate-departures
```

</TabItem>
</Tabs>

After successful execution you can check the schema and data of our table in Polynote. 
The new column `dl_ts_captured` shows the current time of the data pipeline run when this object first occurred in the input data. 

    dataIntDepartures.getSparkDataFrame().printSchema

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

We can check the work of DeduplicateAction by the following query in Polynote: 

    dataIntDepartures.getSparkDataFrame()
    .groupBy($"icao24", $"estdepartureairport", $"dt")
    .count
    .orderBy($"count".desc)
    .show

    +------+-------------------+--------+-----+
    |icao24|estdepartureairport|      dt|count|
    +------+-------------------+--------+-----+
    |4b43ab|               LSZB|20210829|    3|
    |4b4b8d|               LSZB|20210829|    3|
    |4b1b13|               LSZB|20210829|    2|
    |4b4445|               LSZB|20210829|    2|
    |4b0f70|               LSZB|20210830|    1|
    |4b1a01|               LSZB|20210829|    1|
    |346603|               LSZB|20210829|    1|
    |4b4442|               LSZB|20210829|    1|
    |4d02d7|               LSZB|20210829|    1|
    |4b43ab|               LSZB|20210830|    1|
    ...

... and it seems that it did not work properly! There are 2 or even 3 records for the same primary key!
Even worse, we just deleted this table before, so DeduplicateAction shouldn't have any work to do at all.

In fact DeduplicateAction assumes that input data is already unique for the given primary key. 
This would be the case for example, in a messaging context, if you were to receive the same message twice.
DeduplicateAction doesn't deduplicate your input data again, because deduplication is costly and data often is already unique.
But in our example we have duplicates in the input data set, and we need to add some deduplication logic to our input data (this will probably become a configuration flag in future SDL version, see issue [#428](https://github.com/smart-data-lake/smart-data-lake/issues/428)).

As the easiest way to do this is by using the Scala Spark API, we will add a second ScalaCodeSparkDfTransformer as follows (make sure you get the brackets right): 

    deduplicate-departures {
      type = DeduplicateAction
      ...
      transformers = [{
        type = SQLDfTransformer
        code = "select stg_departures.*, date_format(from_unixtime(firstseen),'yyyyMMdd') dt from stg_departures"
      },{
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
      ...
    }

If you run Action `deduplicate-departures` again and check the result in Polynote, everything is fine now.

:::info
Note how we have used a third way of defining transformation logic now:  
In part 1 we first used a SQLDfsTransformer writing SQL code.   
Then for the more complex example of computing distances, we used a  ScalaClassSparkDfTransformer pointing to a Scala class.   
Here, we simply include Scala code in our configuration file directly.
:::

For sure DeduplicateAction did not have much work to do, as this was the first data load. 
In order to get different data you would need to adjust the unix timestamp parameters in the URL of DataObject `ext-departures`. 
Feel free to play around.

:::info Scala Code
Scala is a compiled language. The compiler creates bytecode which can be run on a JVM.
Normally compilation takes place before execution. So how does it work with scala code in the configuration as in our deduplication logic above?

With Scala, you can compile code on the fly. This is actually what the Scala Shell/REPL is doing as well. 
The Scala code in the configuration above gets compiled when ScalaCodeSparkDfTransformer is instantiated during startup of SDL.
:::

## Summary

You have now seen different parts of industrializing a data pipeline like robust data formats and caring about historical data.
Further, you have explored data interactively with a notebook. 

The final configuration file of Part 2 should look like [this](../config-examples/application-part2-historical.conf)

In part 3 we will see how to incrementally load fresh flight data and optimize deduplication and historization.
See you!
