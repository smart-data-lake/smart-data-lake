---
title: Select Columns
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Goal

In this step we write our first Action that modifies data.
We will continue based upon the config file available [here](../config-examples/application-part1-download.conf).
When you look at the data in the folder *data/stg-airports/result.csv*, you will notice that we
don't need most of the columns. In this step, we will write a simple *CopyAction* that selects only the columns we
are interested in.

As usual, we need to define an output DataObject and an action. 
We don't need to define a new input DataObject as we will wire our new action to the existing DataObject *stg-airports*. 

## Define output object

Let's use CsvFileDataObject again because that makes it easy for us to check the result.
In more advanced (speak: real-life) scenarios, we would use one of numerous other possibilities, 
such as HiveTableDataObject, SplunkDataObject...
See [this list](https://github.com/smart-data-lake/smart-data-lake/blob/develop-spark3/docs/Reference.md#data-objects) for an overview.
You can also consult the [Configuration Schema Browser](https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2) to get a list of all Data Objects and related properties.

In a first step, we want to make the airport data more understandable by removing any columns we don't need. 
Since we don't introduce any business logic into the transformation, 
the resulting data object will reside in the integration layer and thus will be called *int-airports*.
Put this in the existing dataObjects section:

      int-airports {
        type = CsvFileDataObject
        path = "~{id}"
      }

## Define select-airport-cols action 

Next, add these lines in the existing actions section:

      select-airport-cols {
        type = CopyAction
        inputId = stg-airports
        outputId = int-airports
        transformers = [{
            type = SQLDfTransformer
            code = "select ident, name, latitude_deg, longitude_deg from stg_airports"
        }]
        metadata {
          feed = compute
        }
      }
A couple of things to note here:

- We just defined a new action called *select-airport-cols*. 
- We wired it together with the two DataObjects *stg-airports* and *int-airports*.
- A new type of Action was used: CopyAction. This action is intended to copy data from one data object to another
with some optional transformations of the data along the way.
- To define the transformations of an action, you define a list of HOCON Objects.
HOCON-Objects are just like JSON-Objects (with a few added features, but more on that later).
- Instead of allowing for just one transformer, we could potentially have multiple transformers within the same action that
  get executed one after the other. That's why we have the bracket followed by the curly brace `[{` :
  the CustomSparkAction expects it's field *transformers* to be a list of HOCON Objects.
- There's different kinds of transformers, in this case we defined a *SQLDfTransformer* and provided it with a custom SQL-Code.
There are other transformer types such as *ScalaCodeDfTransformer*, *PythonCodeDfTransformer*... More on that later.

:::caution

Notice that we call our input DataObject stg-airports with a hyphen "-", but in the sql, we call it "stg\_airports" with an underscore "\_".
This is due to the SQL standard not allowing "-" in unquoted identifiers (e.g. table names). 
Under the hood, Apache Spark SQL is used to execute the query, which implements SQL standard.
SDL works around this by replacing special chars in DataObject names used in SQL statements for you. 
In this case, it automatically replaced `-` with `_`

:::

There are numerous other options available for the CopyAction, which you can view in the [Configuration Schema Browser](https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=3-0).



## Try it out
Note that we used a different feed this time, we called it *compute*. 
We will keep expanding the feed *compute* in the next few steps.
This allows us to keep the data we downloaded in the previous steps in our local files and just
try out our new actions.

To execute the pipeline, use the same command as before, but change the feed to compute:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel compute
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel compute
```

</TabItem>
</Tabs>

:::caution

If you encounter an error that looks like this:

    Exception in thread "main" io.smartdatalake.util.dag.TaskFailedException: Task select-airport-cols failed. 
			Root cause is 'IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject schema 
			is undefined. A schema must be defined if there are no existing files.'
    Caused by: java.lang.IllegalArgumentException: requirement failed: (DataObject~stg-airports) DataObject 
			schema is undefined. A schema must be defined if there are no existing files.

Execute the **`download`**-feed again. After that feed was successfully executed, the execution of the feed `.*` or `compute` will work.
More on this problem in the list of [Common Problems](../troubleshooting/common-problems.md).


:::

Now you should see multiple files in the folder *data/int-airports*. Why is it split accross multiple files?
This is due to the fact that the query runs with Apache Spark under the hood which computes the query in parallel for different portions of the data.
We might work on a small data set for now, but keep in mind that this would scale up horizontally for large amounts of data.

## More on Feeds

SDL gives you precise control on which actions you want to execute. 
For instance if you only want to execute the action that we just wrote, you can type

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:select-airport-cols
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel ids:select-airport-cols
```

</TabItem>
</Tabs>

Of course, at this stage, the feed *compute* only contains this one action, so the result will be the same.

SDL also allows you to use combinations of expressions to select the actions you want to execute. You can run

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm sdl-spark:latest --help
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm sdl-spark:latest --help
```

</TabItem>
</Tabs>

to see all options that are available. For your convenience, here is the current output of the help command:

      -f, --feed-sel <operation?><prefix:?><regex>[,<operation?><prefix:?><regex>...]
                               Select actions to execute by one or multiple expressions separated by comma (,). Results from multiple expressions are combined from left to right.
                               Operations:
                               - pipe symbol (|): the two sets are combined by union operation (default)
                               - ampersand symbol (&): the two sets are combined by intersection operation
                               - minus symbol (-): the second set is subtracted from the first set
                               Prefixes:
                               - 'feeds': select actions where metadata.feed is matched by regex pattern (default)
                               - 'names': select actions where metadata.name is matched by regex pattern
                               - 'ids': select actions where id is matched by regex pattern
                               - 'layers': select actions where metadata.layer of all output DataObjects is matched by regex pattern
                               - 'startFromActionIds': select actions which with id is matched by regex pattern and any dependent action (=successors)
                               - 'endWithActionIds': select actions which with id is matched by regex pattern and their predecessors
                               - 'startFromDataObjectIds': select actions which have an input DataObject with id is matched by regex pattern and any dependent action (=successors)
                               - 'endWithDataObjectIds': select actions which have an output DataObject with id is matched by regex pattern and their predecessors
                               All matching is done case-insensitive.
                               Example: to filter action 'A' and its successors but only in layer L1 and L2, use the following pattern: "startFromActionIds:a,&layers:(l1|l2)"
      -n, --name <value>       Optional name of the application. If not specified feed-sel is used.
      -c, --config <file1>[,<file2>...]
                               One or multiple configuration files or directories containing configuration files, separated by comma. Entries must be valid Hadoop URIs or a special URI with scheme "cp" which is treated as classpath entry.
      --partition-values <partitionColName>=<partitionValue>[,<partitionValue>,...]
                               Partition values to process for one single partition column.
      --multi-partition-values <partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>[;(<partitionColName1>=<partitionValue>,<partitionColName2>=<partitionValue>;...]
                               Partition values to process for multiple partitoin columns.
      -s, --streaming          Enable streaming mode for continuous processing.
      --parallelism <int>      Parallelism for DAG run.
      --state-path <path>      Path to save run state files. Must be set to enable recovery in case of failures.
      --override-jars <jar1>[,<jar2>...]
                               Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath.
      --test <config|dry-run>  Run in test mode: config -> validate configuration, dry-run -> execute prepare- and init-phase only to check environment and spark lineage
      --help                   Display the help text.
      --version                Display version information.
      -m, --master <value>     The Spark master URL passed to SparkContext (default=local[*], yarn, spark://HOST:PORT, mesos://HOST:PORT, k8s://HOST:PORT).
      -x, --deploy-mode <value>
                               The Spark deploy mode passed to SparkContext (default=client, cluster).
      -d, --kerberos-domain <value>
                               Kerberos-Domain for authentication (USERNAME@KERBEROS-DOMAIN) in local mode.
      -u, --username <value>   Kerberos username for authentication (USERNAME@KERBEROS-DOMAIN) in local mode.
      -k, --keytab-path <value>
                               Path to the Kerberos keytab file for authentication in local mode.


One popular option is to use regular expressions to execute multiple feeds together.
In our case, we can run the entire data pipeline with the following command : 

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel .*
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel .*
```

</TabItem>
</Tabs>



## Example of Common Mistake

One common mistake is mixing up the types of Data Objects.
To give you some experience on how to debug your config, you can also try out what happens if you change the type of *stg-airports* to JsonFileDataObject.
You will get an error message which indicates that there might be some format problem, but it is hard to spot :

     Error: cannot resolve '`ident`' given input columns: [stg_airports._corrupt_record]; line 1 pos 7;

The FileTransferAction will save the result from the Webservice with the JsonFileDataObject as file with filetype \*.json. 
Then Spark tries to parse the CSV-records in the \*.json file with a JSON-Parser. It is unable to properly read the data.
However, it generates a column named *_corrupt_record* describing what went wrong. 
If you know Apache Spark, this column will look very familiar to you.
After that, the query fails, because it only finds that column with error messages instead of the actual data.

One way to get a better error message is to tell Spark that it should promptly fail when reading a corrupt file.
You can do that with the option [jsonOptions](https://smartdatalake.ch/json-schema-viewer/index.html#viewer-page?v=2-13-3),
which allows you to directly pass on settings to Spark.

In our case, we would end up with a faulty dataObject that looks like this:

      stg-airports {
        type = JsonFileDataObject
        path = "~{id}"
        jsonOptions {
          "mode"="failfast"
        }
      }

This time, it will fail with this error message:

    Exception in thread "main" io.smartdatalake.workflow.TaskFailedException: Task select-airport-cols failed. 
    Root cause is 'SparkException: Malformed records are detected in schema inference. 
    Parse Mode: FAILFAST. Reasons: Failed to infer a common schema. Struct types are expected, but `string` was found.'

