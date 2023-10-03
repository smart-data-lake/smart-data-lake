---
title: Delta Lake - a better data format
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

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
Smart Data Lake Builder supports this by the HiveTableDataObject. It always uses Parquet file format in the background as a best practice, although Hive tables could also be created on top of CSV files.

:::info
Hive is a Metadata layer and SQL engine on top of a Hadoop filesystem. Spark uses the metadata layer of Hive, but implements its own SQL engine.
:::

## Transactions

Hive tables with Parquet format are lacking transactions. This means for example that writing and reading the table at the same time could result in failure or empty results. 
In consequence 
* consecutive jobs need to by synchronized
* it's not recommended having end-user accessing the table while data processing jobs are running
* update and deletes are not supported

There are other options like classical databases which always had a metadata layer, offer transactions but don't integrate easily with Hive metastore and cheap, scalable Hadoop file storage.
Nevertheless, Smart Data Lake Builder supports classical databases through the JdbcTableDataObject.
Fortunately there is a new technology called Delta Lake, see also [delta.io](https://delta.io/). It integrates into a Hive metastore, supports transactions and stores Parquet files and a transaction log on hadoop filesystems.
Smart Data Lake Builder supports this by the DeltaLakeTableDataObject, and this is what we are going to use for our airport and departure data now.

## DeltaLakeTableDataObject

Switching to Delta Lake format is easy with Smart Data Lake Builder, just replace `CsvFileDataObject` with `DeltaLakeTableDataObject` and define the table's db and name.
Let's start by changing the existing definitions for `int-airports`, `btl-departures-arrivals-airports` and `btl-distances`:

    int-airports {
        type = DeltaLakeTableDataObject
        path = "~{id}"
        table = {
            db = "default"
            name = "int_airports"
        }
    }

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

Then create a new, similar data object `int-departures`: 

    int-departures {
        type = DeltaLakeTableDataObject
        path = "~{id}"
        table = {
            db = default
            name = int_departures
        }
    }
    
Next, create a new action `prepare-departures` in the `actions` section to fill the new table with the data:

    prepare-departures {
        type = CopyAction
        inputId = stg-departures
        outputId = int-departures
        metadata {
            feed = compute
        }
    }

Finally, adapt the action definition for `join-departures-airports`:
* change `stg-departures` to `int-departures` in inputIds
* change `stg_departures` to `int_departures` in the first SQLDfsTransformer (watch out, you need to replace the string 4 times)

:::info Explanation
- We changed `int-airports` from CSV to Delta Lake format
- Created an additional table `int-departures`
- Created an action `prepare-departures`  to fill the new integration layer table `int-departures`
- Adapted the existing action `join-departures-airports` to use the new table `int-departures`
:::

To run our data pipeline, first make sure data directory is empty - otherwise DeltaLakeTableDataObject will fail because of existing files in different format.
Then you can execute the usual *docker run* command for all feeds:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
mkdir data
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel 'download*'
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel '^(?!download).*'
```

</TabItem>
<TabItem value="podman">

```jsx
mkdir data
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel 'download*'
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest -c /mnt/config --feed-sel '^(?!download).*'
```

</TabItem>
</Tabs>

:::info
Why two separate commands?   
Because you deleted all data first.   

Remember from part 1 that we either need to define a schema for our downloaded files or we need to execute the download steps separately on the first run.
The first command only executes the download steps, the second command executes everything but the download steps (regex with negative lookahead).
See [Common Problems](../troubleshooting/common-problems.md) for more Info.
:::


Getting an error like `io.smartdatalake.util.webservice.WebserviceException: Read timed out`? Check the list of [Common Problems](../troubleshooting/common-problems) for a workaround.

## Reading Delta Lake Format with Spark

Checking our results gets more complicated now - we can't just open delta lake format in a text editor like we used to do for CSV files.
We could now use SQL to query our results, that would be even better. 
One option is to use a Spark session, i.e. by starting a spark-shell.
But state-of-the-art is to use notebooks like Jupyter for this.
One of the most advanced notebooks for Scala code we found is Polynote, see [polynote.org](https://polynote.org/).

We will now start Polynote in a docker container, and an external Metastore (Derby database) in another container to share the catalog between our experiments and the notebook.
To do so, we will use the additional files in the subfolder `part2`. 
Execute these commands:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker-compose -f part2/docker-compose.yml -p getting-started build
mkdir -p data/_metastore
docker-compose -f part2/docker-compose.yml -p getting-started up
```

</TabItem>
<TabItem value="podman">

```jsx
mkdir -p data/_metastore
./part2/podman-compose.sh 
```

</TabItem>
</Tabs>

This might take multiple minutes.
You should now be able to access Polynote at `localhost:8192`. 

:::info Docker on Windows
If you use Windows, please read our note on [Docker for Windows](../troubleshooting/docker-on-windows).
You might notice that the commands for docker and podman differ at this point. 
The latest version of podman-compose changed the behavior of creating pods, 
which is why we have implemented a script `podman-compose.sh` to emulate podman-compose.
:::

But when you walk through the prepared notebook "SelectingData", you won't see any tables and data yet. 
Can you guess why?  
This is because your last pipeline run used an internal metastore, and not the external metastore we started with docker-compose yet.
To configure Spark to use our external metastore, add the following spark properties to the application.conf under global.spark-options. 
You probably don't have a global section in your application.conf yet, so here is the full block you need to add at the top of the file:

    global {
      spark-options {
        "spark.hadoop.javax.jdo.option.ConnectionURL" = "jdbc:derby://metastore:1527/db;create=true"
        "spark.hadoop.javax.jdo.option.ConnectionDriverName" = "org.apache.derby.jdbc.ClientDriver"
        "spark.hadoop.javax.jdo.option.ConnectionUserName" = "sa"
        "spark.hadoop.javax.jdo.option.ConnectionPassword" = "1234"
      }
    }

This instructs Spark to use the external metastore you started with docker-compose. 
Your Smart Data Lake container doesn't have access to the other containers just yet. 
So when you run your data pipeline again, you need to add a parameter `--network`/`--pod` to join the virtual network where the metastore is located:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --hostname localhost --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network getting-started_default sdl-spark:latest -c /mnt/config --feed-sel '.*'
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --pod getting-started sdl-spark:latest -c /mnt/config --feed-sel '.*'
```

</TabItem>
</Tabs>

After you run your data pipeline again, you should now be able to see our DataObjects data in Polynote.
No need to restart Polynote, just open it again and run all cells.
[This](../config-examples/application-part2-deltalake.conf) is how the final configuration file should look like. Feel free to play around.

:::tip Delta Lake tuning
You might have seen that our data pipeline with DeltaTableDataObject runs a Spark stage with 50 tasks several times.
This is delta lake reading it's transaction log with Spark. For our data volume, 50 tasks are way too much.
You can reduce the number of snapshot partitions to speed up the execution by setting the following Spark property in your `application.conf` under `global.spark-options`:

    "spark.databricks.delta.snapshotPartitions" = 2
:::

:::tip Spark UI from Polynote
On the right side of Polynote you find a link to the Spark UI for the current notebooks Spark session. 
If it doesn't work, try to replace 127.0.0.1 with localhost. If it still doesn't work, replace with IP address of WSL (`wsl hostname -I`). 
:::

In the next step, we are going to take a look at keeping historical data...
