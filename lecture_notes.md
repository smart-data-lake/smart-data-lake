# Preparation
:warning: do we expect to have WSL installed? How do we proceed without? Do we prepare an example with CSV output, and onlt parquet, thaus we need no metastore and no polynote? (Historize without DeltaLake???)
* download example case
* build SDLB: `podman build -t sdl-spark .`
* try SDLB: `podman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel launchTask --help`


# Smart Data Lake Builder Hands-On Training
> &#x1F4DD; 

Day1 part2: SDLB with example case

## Goal
discover all flights which are no longer than 500km from a given departure (here Bern and Frankfurt)
> see presentation summary

## Setup
* clone repo
> git clone ...

:warning: TODO

## Specifying application

### Hocon
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

What else is supported?
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

## Application execution
* `feed-sel` always necessary (see help `--help`)
	- can be specified by metadata feed, name, or ids
	- can be lists or regex
	- can also be `startWith...` or `endWith`

> first run config test `podman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel 'ids:download-airports' --test config` (fix bug together)


hile running we get:
`Exception in thread "main" io.smartdatalake.config.ConfigurationException: (DataObject~stg-airports) ClassNotFoundException: Implementation CsvDataObject of interface DataObject not found`
let us double check what DataObjects there are available... [SDLB Schema Viewer](http://smartdatalake.ch/json-schema-viewer/index.html#viewer-page&version=sdl-schema-2.3.0-SNAPSHOT.json)

> fix issue by correcting the dataObject type to `CvsFileDataObject`

> run again (and then with) `--test dry-run`: `podman run --rm --hostname=localhost --pod getting-started -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config sdl-spark:latest --config /mnt/config --feed-sel '.*' --test dry-run`

### DAG
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

### Execution Phases
* logs reveal the **execution phases**
* in general we have: 
	- configuration parsing
	- DAG preparation
	- DAG init
	- DAG exec
* early validation: in init even custom transformation are checked, e.g. identifying mistakes in column names
* [website link]()

#### incremental

```
    executionMode = { type = DataObjectStateIncrementalMode }
    mergeModeEnable = true
    updateCapturedColumnOnlyWhenChanged = true
 ```
:warning: TODO

### 