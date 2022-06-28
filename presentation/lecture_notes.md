
# Smart Data Lake Builder Hands-On Training
> &#x1F4DD; 

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
  - store processed data in data lake, thus can be utilized for various use cases
  - analyse/compute data for a specific application


## Why Smart Data Lake?
* also called Lakehouse
* combining the flexibility of Data Lakes with the advantages of Data Warehouses

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

## Lakehouse Plattform Comparison
* Lakehouse implementations: Snowflake - DBT, Azure Data Factory, Apache Beam, …


### Why Smart Data Lake Builder (SDLB)?
* No Code for easy tasks
* Complex data pipelines
* Designed to add custom connectors and transformations
* various data formats, incl. DeltaLake
* Lineage and Data Catalog from metadata
* supports incremental load, historize & upsert/merge, Schema evolution (on specific data formats), Partition-wise processing, streaming, checkpoint & recovery, ...
* DevOps ready: versionable configuration, support for automated testing

:warning: TODO comparison e.g. with Azure Data Factory see PowerPoint or confluence

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

## Security
* protect data especially in the cloud 
* manage access using permission groups
  - Users belong to role groups
  - role groups have permission groups
  - permission groups mange permissions for apps, devices, and environments

![permission managment from user over role groups and technical troups to specify permissions](images/authorisationConcept.png)

:warning: TODO


## Setup
* clone repo
> git clone ...

:warning: TODO

## Let's have a look to the actual impementation
We have already something prepared...

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

## Databricks

### setup
* uploading files
* start job

### Notebook Support

:warning: TODO