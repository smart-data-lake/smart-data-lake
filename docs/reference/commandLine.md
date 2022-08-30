---
id: commandLine
title: Command Line
---

# Start Java object using Spark-Submit
SmartDataLakeBuilder is a java application. 
To run on a cluster with spark-submit, use **DefaultSmartDataLakeBuilder** application.
It can be started with the following command line options (for details, see [YARN](deployYarn.md)).

```bash
spark-submit --master yarn --deploy-mode client --class io.smartdatalake.app.DefaultSmartDataLakeBuilder target/smartdatalake_2.11-1.0.3-jar-with-dependencies.jar [arguments]
```
and takes the following arguments:
```
Usage: DefaultSmartDataLakeBuilder [options]
  -f, --feed-sel &ltoperation?&gt&ltprefix:?&gt&ltregex&gt[,&ltoperation?&gt&ltprefix:?&gt&ltregex&gt...]
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
  -n, --name &ltvalue&gt       Optional name of the application. If not specified feed-sel is used.
  -c, --config &ltfile1&gt[,&ltfile2&gt...]
    One or multiple configuration files or directories containing configuration files, separated by comma. Entries must be valid Hadoop URIs or a special URI with scheme "cp" which is treated as classpath entry.
  --partition-values &ltpartitionColName&gt=&ltpartitionValue&gt[,&ltpartitionValue&gt,...]
    Partition values to process for one single partition column.
  --multi-partition-values &ltpartitionColName1&gt=&ltpartitionValue&gt,&ltpartitionColName2&gt=&ltpartitionValue&gt[;(&ltpartitionColName1&gt=&ltpartitionValue&gt,&ltpartitionColName2&gt=&ltpartitionValue&gt;...]
    Partition values to process for multiple partitoin columns.
  -s, --streaming          Enable streaming mode for continuous processing.
  --parallelism &ltint&gt      Parallelism for DAG run.
  --state-path &ltpath&gt      Path to save run state files. Must be set to enable recovery in case of failures.
  --override-jars &ltjar1&gt[,&ltjar2&gt...]
    Comma separated list of jar filenames for child-first class loader. The jars must be present in classpath.
  --test &ltconfig|dry-run&gt  Run in test mode: config -&gt validate configuration, dry-run -&gt execute prepare- and init-phase only to check environment and spark lineage
  --help                   Display the help text.
  --version                Display version information.
```


The **DefaultSmartDataLakeBuilder** class should be fine in most situations.
There are two additional, adapted application versions you can use:
- **LocalSmartDataLakeBuilder**: default for Spark master is `local[*]` in this case, and it has additional properties to configure Kerberos authentication. 
Use can use this application to run in a local environment (e.g. IntelliJ) without cluster deployment.
- **DatabricksSmartDataLakeBuilder**: see [Microsoft Azure](deploy-microsoft-azure.md), special class when running a Databricks Cluster.

# Launching SDL container
Depending on the container defintion, especially the entrypoint the arguments may vary. Furthermore, we distinguish starting the container using *docker* or *podman*. 

In general a container launch would look like:

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run [docker-args] sdl-spark --config [config-file] --feed-sel [feed] [further-SDL-args]
```

</TabItem>
<TabItem value="podman">

```jsx
podman run [docker-args] sdl-spark --config [config-file] --feed-sel [feed] [further-SDL-args]
```

</TabItem>
</Tabs>

These could also include mounted directories for configurations, additional Scala Classes, data directories, etc.

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download
```

</TabItem>
<TabItem value="podman">

```jsx
podman run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel download
```

</TabItem>
</Tabs>


## Pods with Podman
When interacting between multiple containers, e.g. SDL container and a metastore container, pods are utilized to manage the container and especially the network. A set of containers is launched using podman-compose and a `docker-compose.yaml`. Podman-compose < 1.0.0 creates the pods automatically. This seems to be broken in later versions. That is why we suggest to install podman-compose 0.1.11.

Assuming an existing pod `mypod` is running, another container can be started within this pod using the additional podman arguments `--pod mypod --hostname myhost --add-host myhost:127.0.0.1`. 
The hostname specification fixes an issue in resolving the own localhost.



