
# Smart Data Lake Builder Hands-On Training

## Goal
* tain loving friend imagine to replace short distant flights with train rides
* discover/count all flights which 
  - starting from a certain airport 
  - <500km
  - asumme destination airport Bern (LSZB) or Frankfurt (EDDF)

![map about flights around Bern](images/flights_BE.png)

* as data engineers we need to:
  - download data
  - combining/filter/transform data in a general manner
  - store processed data, thus it can be utilized for various use cases
  - analyse/compute data for a specific application

## Smart Data Lake vs. Smart Data Lake Builder

|Smart Data Lake | Smart Data Lake Builder |
|----------------|-------------------------|
| **Concept**    | **Tool**                |
| combines advantages of Data Warehouse and Data Lakes | ELCA's tool to build Data Pipelines efficiently |
| structured data / schema evolution | portable and automated |
| layer approach / processes / technologies | features like historization, incremental load, partition wise |
| Similar to Lakehouse | Open Source on Github: [smart-data-lake](https://github.com/smart-data-lake/) |

## Why Smart Data Lake?
* similar to Lakehouse concept
* combining the flexibility of Data Lakes with the advantages of Data Warehouses
Let's have a closer look.

### Data Warehouse
  - :heavy_plus_sign: preface preparation, structuring and securing data for high quality reporting
  - :heavy_minus_sign: slow changes, less flexible
  - :heavy_minus_sign: no horizontal scaling
  - :heavy_minus_sign: high license cost, expensive
  - :heavy_minus_sign: no unstructured data processing
  - :heavy_minus_sign: insufficient integration of AI
### Data Lake
  - :heavy_plus_sign: performant and flexible
  - :heavy_plus_sign: scalable, open source
  - :heavy_plus_sign: unstructured and complex data structures
  - :heavy_minus_sign: siloed initatives, redundancy
  - :heavy_minus_sign: heavy data preparation for every use case

### Smart Data Lake aka. Lakehouse
  - :heavy_plus_sign: value & semantic oriented
  - :heavy_plus_sign: known data quality
  - :heavy_plus_sign: secured, handled privacy
  - :heavy_plus_sign: Metadata (data cataloge & linage)
  - :heavy_plus_sign: automation, unified batch and streaming
  - :heavy_plus_sign: AI ready

![data plattforms comparison](images/smartDataLake-dataPlattforms.png)

### Why Smart Data Lake Builder (SDLB)?
* examples of other tools: Snowflake - DBT, Azure Data Factory, Apache Beam, …
  - able to use Smart Data Lake concepts
  - different advantages and disadvanteges for building data pipelines
* dynamic creation of workflow (no specification of step ordering)
* No Code for easy tasks
* complex data pipelines well suited
* already implemented loading modes (incremental, patition-wise, streaming, checkpoint/recovery)
* already implemented transformations (historize, deduplication)
* Designed to add custom connectors and transformations
* various data formats, incl. DeltaLake
* Lineage and Data Catalog from metadata
* DevOps ready: versionable configuration, support for automated testing
* early validation
* scripted -> portable can run on most (manybe any) cloud or on-prem platform
* configuration templates allows reuse of configuration blocks

## Data structuring
Within Smart Data Lake we structure our data in layers

* stage layer 
  - copy of the original data, accessible for merging/combining with exisiting/other data sources
* integration layer 
  - cleaning/structuring/prepared data
* business transformation layer
  - ready for data analysts/scientists to run/develop their applications

In our case we could think of the following structure:

![data layer structure for the use case](images/dataLayers.png)

## Security In Cloud
* typically data need to be protected against unwanted access and modification, especially in the cloud, where data is by default accessible from everywhere (from everyone, if misconfigured)
* Data Classification: Public, Internal, Restricted, Personal Data, Confidential
  - various security measures per class, including (not the full list):
    + access control
      * strong authentication
      * periodic review of permissions
    + cryptography
      * encrypt at rest and in transit
      * elaborated secret stores, for key managment
    + operations security
      * documentation and automation (CD), change management
      * seperated environments with configuration and capacity managment
    + ...
    + [ELCAs Cloud Security Concept](https://confluence.svc.elca.ch/display/BL9CONFLUENCE/Data+Security+Concept+for+Cloud+Analytics+Platforms)

  - seperate treatment of Personally Identifiable Information (PII)
    + e.g. names, address, social sec. numbers, tel.numbers, medical/health data, ...
    + anonymisation (best), pseudonymisation 
    + additional encrytion (in use)
    + further regulations: right of data removal, data location restrictions, etc.
    + ...
    + [ELCAs Cloud Data Privacy Concept](https://confluence.svc.elca.ch/display/BL9CONFLUENCE/Technical+Aspects+of+Data+Privacy+Concepts+for+Cloud+Analytics+Platforms)
* authorisation managment using roles and permission groups
  - Users belong to role groups
  - role groups have permission groups
  - permission groups mange permissions for apps, devices, and environments
  - ...
  - [ELCAs Cloud Authorisation Concept](https://confluence.svc.elca.ch/display/BL9CONFLUENCE/Authorization+Concept+for+Cloud+Analytics+Platforms)

![permission managment from users utilizing role groups and technical troups to specify permissions](images/authorisationConcept.png)

:warning: TODO


## Setup
* clone repo
> git clone ...

:warning: TODO

## Let's have a look to the actual impementation
We have already something prepared...

## Hocon
* Human-Optimized Config Object Notation
* originating from JSON

> list config directory

* config can be splitted
* can also be used for managing different entwironments (e.g. `--config ./config/prod,config/global.conf`)

> `nano config/airports.conf`

* here we see an example with 3 stages, 3 data types and 2 actions to get from one to the other

* parameters and structures
* `ext-airports` specifies the location of a file to be downloaded and `stg-airport` the destination to be downloaded
* further the data will be filtered and written into `DeltaLakeTable`
* Schema inference
	- support for schema evolution, depending on the Action schema will be replaced or extended (new column added, removed columns kept)
		+ not for partitioned Hive tables


<!--
## Excursion: env variables
- usage of optional env variables
  ```Hocon
  basedir = "/whatever/whatever"
  basedir = ${?FORCED_BASEDIR}
  ```
- overwrite prameters with env variables
  + specify java option `-Dconfig.override_with_env_vars=true` in Docker entrypoint and
  + env var:
    * prefix `CONFIG_FORCE_` is stripped
    * single underscore(`_`) is converted into a dot(`.`)
    * double underscore(`__`) is converted into a dash(`-`)
    * triple underscore(`___`) is converted into a single underscore(`_`)
:warning: TODO overwrite not working'
-->

### What else is supported?
> open [SDLB Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html#viewer-page&version=sdl-schema-2.3.0-SNAPSHOT.json)
* distinguish `gloabl`, `dataObjects`, `actions`, and `connections`

### DataObjects
mention a few dataObjects: 

* `AirbyteDataObject` provides access to a growing list of [Airbyte](https://docs.airbyte.com/integrations/) connectors to various sources and sinks e.g. Facebook, Google {Ads,Analytics,Search,Sheets,...}, Greenhouse, Instagram, Jira,...
* `JdbcTableDataObject` to connect to a database e.g. MS SQL or Oracle SQL
* `DeltaLakeTableDataObject` tables in delta format (based on parquet), including shema registered in metastore and transaction logs enables time travel (a common destination)
* `SnowflakeTableDataObject` access to Snowflake tables 

### Actions
* not as many as dataObjects, but very flexible.
* basis action with additional default functionality like, Deduplication and Historization
* all further logic is defined in the action as transformer
* 1to1 (files) up to  manay to many

#### Transformations
* there are 1to1 and many-to-many transformations
* transformers supports languages:
	- ScalaClass
	- ScalaCode
	- SQL
	- Python
* predefined transformers, e.g.:
	- `AdditionalColumnsTransformer` (in HistorizeAction), adding information from context or derived from input, for example, adding input file name
	- `SparkRepartitionTransformer`

Let's have a closer look to the present examples:
> open `config/departures.conf` look at `download-deduplicate-departures`

* **chained** transformers
* first **SQL** query, to convert UnixTime to dateTime format
* then **Scala Code** for deduplication
	- the deduplication action does compare input and target
	- The transformation verifies that there are no duplicated in the input

* there is also Scala Class used in the example, but we will not go into detail yet.

## Feeds
* start application with `--help`: `podman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config`

> Note: `-rm` removes container after exit, `hostname` and `pod` for lauching in same Network as metastore and Polynote, mounting data, target and config directory, container name, config directories/files

* `feed-sel` always necessary 
	- can be specified by metadata feed, name, or ids
	- can be lists or regex, e.g. `--feed-sel '.*'`
	- can also be `startWith...` or `endWith...`

## Environment Variables in HOCON
* try run feed everything: 
`podman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel '.*'`

* error: `Could not resolve substitution to a value: ${METASTOREPW}`
  - in `config/global.conf` we defined `"spark.hadoop.javax.jdo.option.ConnectionPassword" = ${METASTOREPW}`
  - -> use environmentvariable `-e METASTOREPW=1234`

## Test Configuration
* first run config test: 
`podman run -e METASTOREPW=1234 --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel 'download' --test config` (fix bug together)

* while running we get:
`Exception in thread "main" io.smartdatalake.config.ConfigurationException: (DataObject~stg-airports) ClassNotFoundException: Implementation CsvDataObject of interface DataObject not found`
let us double check what DataObjects there are available... [SDLB Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html#viewer-page&version=sdl-schema-2.3.0-SNAPSHOT.json)

> Task: fix issue by correcting the dataObject type to `CvsFileDataObject`

## Dry-run
* run again (and then with) `--test dry-run` and feed `'.*'` to check all configs: 
  `podman run -e METASTOREPW=1234 --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel '.*' --test dry-run`

## DAG
* (Directed acyclic graph)
> show DAG in output
* automatically created using the specifications in the SDLB config. 
* can fork and join
* no recursion

```
                        ┌─────┐                                                       
                        │start│
                        └─┬─┬─┘
                          │ │
                          │ └─────────┐
                          v           │
           ┌─────────────────┐        │
           │download-airports│        │
           └────────┬────────┘        │
                    │                 │
           ┌────────┘                 │
           │                          │
           v                          v
 ┌──────────────────┐ ┌───────────────────────────────┐
 │historize-airports│ │download-deduplicate-departures│
 └─────────┬────────┘ └────────┬──────────────────────┘
           │                   │
           └───────────┐       │
                       │       │
                       v       v
              ┌────────────────────────┐
              │join-departures-airports│
              └────────────┬───────────┘
                           │
                           v
                  ┌─────────────────┐
                  │compute-distances│
                  └─────────────────┘
```

## Execution Phases
* logs reveal the **execution phases**
* in general we have: 
	- configuration parsing
	- DAG preparation
	- DAG init
	- DAG exec (not processed in dry-run mode)
* early validation: in init even custom transformation are checked, e.g. identifying mistakes in column names
* [Docu: execution phases](https://smartdatalake.ch/docs/reference/executionPhases)

## Inspect result
During the Airport download we created a CSV file: `less data/stg-airports/results.csv`
The departures are directly loaded into a delta table: open [Polynote at localhost:8192](http://localhost:8192/notebook/SelectingData.ipynb)
  `departure table consits of 457 row and entries are of original date: 20210829 20210830`

## Incremental Load
* desire to **not read all** data from input at every run -> incrementally
* or here: departure source **resricted request** to <7 days
  - initial request 2 days 29.-20.08.2021

### General aspects
* in general we often want an inital load and then regular updates
* distinguish between
* **StateIncremental** 
  - stores a state, utilized during request submission, e.g. WebService or DB request
* **SparkIncremental**
  - uses max values from **compareCol**
 
### Current Example
* here we use state to store the last position
  - [CustomWebserviceDataObject](https://github.com/smart-data-lake/getting-started/blob/training/src/main/scala/io/smartdatalake/workflow/dataobject/CustomWebserviceDataObject.scala) the StateIncremental mode is enabled, by: 
    + using the proper trait for the class
    + instantiating state variables and 
    + defining the setState and getState routines
    + see also the [documentation](https://smartdatalake.ch/docs/getting-started/part-3/incremental-mode)

* enabled by adding to the `download-deduplicate-departures` action:
  ```
    executionMode = { type = DataObjectStateIncrementalMode }
    mergeModeEnable = true
    updateCapturedColumnOnlyWhenChanged = true
  ```
  - and add `--state-path /mnt/data/state -n getting-started` to the command line arguments

* **first run** creates `less data/state/succeeded/getting-started.1.1.json` 
  - see line `"state" : "[{\"airport\":\"LSZB\",\"nextBegin\":1630310979},{\"airport\":\"EDDF\",\"nextBegin\":1630310979}]"`
    + > other content we regard later
  - this is used for the next request
* see next request in output of **next run**:
  `CustomWebserviceDataObject - Success for request https://opensky-network.org/api/flights/departure?airport=LSZB&begin=1631002179&end=1631347779 [exec-download-deduplicate-departures]`
  - also check the [increasing amount fo lines collected in table](http://localhost:8192/notebook/inspectData.ipynb#Cell4)
> run a couple of times

> :warning: When we get result/error: `Webservice Request failed with error <404>`, if there are no new data avalable. 

## Streaming
* continious processing, cases we want to run the actions again and again

### Command Line
* command line option `-s` or `--streaming`, streaming all selected actions
  - requires `--state-path` to be set
* just start `podman run -e METASTOREPW=1234 --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --hostname=localhost --pod getting-started sdl-spark:latest --config /mnt/config/  --feed-sel download --state-path /mnt/data/state -n getting-started -s` and see the action runnning again and again
  - > notice the recurring of both actions, here in our case we could limit the feed to the specific action
  - > monitor the growth of the table
  - > see strieming trigger interval of 48s in output: `LocalSmartDataLakeBuilder$ - sleeping 48 seconds for synchronous streaming trigger interval [main]`
    + change it: search stream in Schema Viewer -> `global`->`synchronousStreamingTriggerIntervalSec = 10` -> interval between 2 starts (not end to start)

:warning: TODO other streaming modes???

## Parallelism
* distinguish 2 types of parallelism
  - within a spark job: the amount of Spark tasks, controlled by global option `    "spark.sql.shuffle.partitions" = 2`
  - parallel running DAG actions, by default serial, one by one action
    + see `Action~download-airports[FileTransferAction]: Exec started` and `Action~download-deduplicate-departures[DeduplicateAction]`
    + use command line option `--parallelism 2` to run both tasks in parallel
    + :warning: parallel actions are more difficult to debug
    
## Checkpoint / Restart
* requires states (`--state-path`)
  - `podman run -e METASTOREPW=1234 --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --hostname=localhost --pod getting-started sdl-spark:latest --config /mnt/config/ --feed-sel '.*' --state-path /mnt/data/state -n getting-started` -> cancel run to simulate crash (after download phase)
* stored current state in file: `data/state/getting-started.1.1.json`
  - see the SUCCESS and CANCELED statements
* restart with the same command
  - notice line at the beginning: `LocalSmartDataLakeBuilder$ - recovering application getting-started runId=1 lastAttemptId=1 [main]`
  - notice the changed DAG, no download

:warning: TODO


# Deployment methods
* SDLB can be deployed in various ways on various platforms
* distinguish running SDLB as:
  - java application
  - containerized
* on-prem or in the cloud

* here we saw containerized locally
* during development we often run the java directly using Intellij in Windows
* in the cloud we have also various options: 
  - jar in Databricks 
  - Containers in Kubernetes (AKS)
  - in Virtual Machines

Here, we want to briefly show the Databricks deployment.

## Databricks
Here we have the Databricks setup already prepared, and briefly present the setup, just to give you an idea.

### Preparation steps (not part of the demonstration)
For reference see also: [SDL Deployment on Databricks](https://smartdatalake.ch/blog/sdl-databricks/).
The following setup is already prepared in the elca-dev tenant:

* uploading files
  - upload jar
    + first build fat-jar: `podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -DskipTests  -P fat-jar -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package`
    + upload Databricks `Workspace`->`User`->`<Username>`->`Import`-> link in `(To import a library, such as a jar or egg, click here)`
  - create typesafe fix:
    ```BASH
    cat << EOF >> ./config-install.sh
    #!/bin/bash
    wget -O /databricks/jars/-----config-1.4.1.jar https://repo1.maven.org/maven2/com/typesafe/config/1.4.1/config-1.4.1.jar
    EOF
    databricks fs mkdirs dbfs:/databricks/scripts
    databricks fs cp ./config-install.sh dbfs:/databricks/scripts/
    ```
  - create compute resource:
    + use the uploaded jar
    + use init script: `dbfs:/databricks/scripts/config-install.sh` (uploaded above)
  - upload configs
    + use Linux Databricks CLI: 
    `datbricks fs mkdirs dbfs:/config`
    `databricks fs cp config/departures.conf dbfs:/databricks/config/departures.conf`

* configure job, using the uploaded jar and 
  - parameters: `["-c","file:///dbfs/databricks/config/","--feed-sel","ids:download-deduplicate-departures", "--state-path", "file:///dbfs/databricks/data/state", "-n", "getting-started"]`
  
### Show case
* Workspace -> workflow -> SDLB-train job -> run job
* after finished, show Data -> int-departures table
* show notbook in Workspace

### Further points
* cluster modification/swap possible (scalability)
* recurring schedule
* easy maintainable metastore
