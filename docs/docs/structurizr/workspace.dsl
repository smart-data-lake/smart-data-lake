workspace {

    model {
        user = person "User or System"
        config = element "Config Files" "Hocon" "Main and Environment config files" "folder"
        parsedConfig = element "Config Export" "Json" "Parsed and resolved Config as one Json-file" "file"
        state = element "State Files" "Json" "State of SDLB Jobs stored as Json Files" "folder"
        data = element "Data" "Many formats and technologies" "Data that is read and written by SDLB Jobs, accessed through DataObjects" "database"
        catalog = element "Catalog" "Metastore, Unity Catalog, Database" "Tables, views, comments, primary and foreign keys" "database"
        schemas = element "Schemas, Statistics and Lineage" "Json" "Schemas, statistics and column lineage of DataObjects, exported by dry-runs and the Schema Exporter" "folder"
        ui = softwareSystem "SDLB UI" "Visualizes configuration, lineage and runs (sdl-visualization)" "external"
        sdlb = softwareSystem "SmartDataLakeBuilder" {
            sdlbJob = container "SDLB Job" "" "Java, Scala" {
                configParser = component "Config Parser" "is responsible to parse the config files and translate them into Scala case classes."
                configObjects = group "Top-level Configuration Objects" {
                    actions = component "Actions" "define the transformation between DataObjects" "" "extendable"
                    dataObjects = component "DataObjects" "define the location and format of data" "" "extendable"
                    connections = component "Connections" "Some DataObjects require a Connection to access remote data. Engine connections select and configure the execution engine of an Action." "" "extendable"
                }
                group "Concepts" {
                    dag = component "DAG" "Executes a set of Actions as a `Directed Acyclic Graph`, which is executed by the SDLB Job. Independent Actions are executed in parallel according to the `parallelism` cmd line option."
                    executionMode = component "Execution Mode" "are responsible to select the data to be processed by the SDLB Job" "" "extendable"
                    transformers = component "Transformers" "implement the transformation logic of an Action in SQL, Scala or Python" "" "extendable"
                    authMode = component "Authentication Mode" "are responsible to authenticate with external ressources" "" "extendable"
                    constraintsExpectations = component "Constraints and Expectations" "can be used to ensure Data Quality" "" "extendable"
                    secretProviders = component "Secret Providers" "are responsible to replace secret values in the configuration using different key stores." "" "extendable"
                    housekeepingMode = component "Housekeeping Mode" "is responsible to cleanup or archive outdated data" "" "extendable"
                }
                group "Execution Engines" {
                    sparkEngine = component "Spark" "sdl-spark: Spark DataFrames in a Spark session inside the SDLB Job" "" "engine"
                    sparkConnectEngine = component "Spark Connect" "sdl-sparkconnect: Spark DataFrames on a remote Spark Connect server" "" "engine"
                    sqlEngine = component "SQL" "sdl-sql: SQL statements rendered with SQLGlot and executed in the database" "" "engine"
                    snowparkEngine = component "Snowpark" "sdl-snowflake: Snowpark DataFrames executed in Snowflake" "" "engine"
                    scalaEngine = component "Plain Scala" "sdl-core: lightweight engine without Spark" "" "engine"
                    fileEngine = component "File" "sdl-core: byte streams of files" "" "engine"
                }
                group "Connector Modules" {
                    component "Delta Lake" "sdl-deltalake: Spark implementation of DeltaLakeTableDataObject" "" "connector"
                    component "Iceberg" "sdl-iceberg: Spark implementation of IcebergTableDataObject" "" "connector"
                    component "Kafka" "sdl-kafka: KafkaTopicDataObject" "" "connector"
                    component "Debezium" "sdl-debezium: DebeziumCdcDataObject for change data capture" "" "connector"
                    component "Snowflake" "sdl-snowflake: SnowflakeTableDataObject" "" "connector"
                    component "Azure" "sdl-azure: helpers to access Azure ressources like LogAnalytics" "" "connector"
                    component "Google Cloud" "sdl-gcp: BigQueryTableDataObject" "" "connector"
                }
                group "Customization Hooks" {
                    initPlugin = component "Init Plugin" "is a hook to add additional startup or shutdown logic, like dynamic log configuration." "" "extendable"
                    stateListener = component "State Listeners" "is a hook to recieve and distribute state updates, e.g. writing it to separate tables for reporting." "" "extendable"
                }
                actions -> dataObjects "read/write"
                actions -> transformers "apply"
                actions -> executionMode "applies"
                actions -> connections "select engine by"
                dataObjects -> connections "use"
                dataObjects -> constraintsExpectations "validates"
                dataObjects -> housekeepingMode "applies"
                connections -> authMode "use"
                connections -> sparkEngine "configure"
                connections -> sparkConnectEngine "configure"
                connections -> sqlEngine "configure"
                dag -> actions "executes"
                configParser -> secretProviders "uses"
            }
            configExporter = container "Config Exporter" "Helper command line tool to parse and export a given configuration" "" "helper"
            schemaExporter = container "Schema Exporter" "Helper command line tool to export DataObject schemas and statistics" "" "helper"
            catalogUpdater = container "Catalog Schema Updater" "Helper command line tool to create and evolve tables and views, and apply comments and keys at deploy time" "" "helper"
        }

        user -> sdlbJob "launches"
        user -> configExporter "launches"
        user -> schemaExporter "launches"
        user -> catalogUpdater "launches on deployment"
        configParser -> config "reads"
        dag -> state "uses"
        dag -> schemas "exports in dry-run"
        dataObjects -> data "reads/writes"
        configExporter -> config "reads"
        configExporter -> parsedConfig "write"
        schemaExporter -> config "reads"
        schemaExporter -> schemas "write"
        catalogUpdater -> config "reads"
        catalogUpdater -> schemas "reads"
        catalogUpdater -> catalog "creates/updates"
        ui -> parsedConfig "visualizes"
        ui -> state "visualizes"
        ui -> schemas "visualizes"
    }

    views {
        container sdlb {
            include *
            include ui
            title "Context"
            description ""
        }

        component sdlbJob {
            include *
            title "Components of an SDLB Job"
            description ""
        }

        styles {
            element "helper" {
                background #6bc4eb
            }
            element "engine" {
                background #85bb65
            }
            element "connector" {
                background #85bb65
            }
            element "external" {
                background #999999
            }
            element "folder" {
                shape "Folder"
            }
            element "file" {
                shape "Folder"
            }
            element "database" {
                shape "Cylinder"
            }
            element "extendable" {
                icon "extension.png"
            }
        }

        theme "./theme.json"
    }

}
