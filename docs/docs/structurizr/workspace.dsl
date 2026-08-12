workspace {

    model {
        user = person "User or System"
        config = element "Config Files" "Hocon" "Main and Environment config files" "folder"
        parsedConfig = element "Config Export" "Json" "Parsed and resolved Config as one Json-file" "file"
        state = element "State Files" "Json" "State of SDLB Jobs stored as Json Files" "folder"
        data = element "Data" "Many formats and technologies" "Data that is read and written by SDLB Jobs, accessed through DataObjects" "database"
        schemas = element "Schemas and Statistics" "Json" "Collection of Schemas and Statistics about DataObjects" "folder"
        sdlb = softwareSystem "SmartDataLakeBuilder" {
            sdlbJob = container "SDLB Job" "" "Java, Spark" {
                configParser = component "Config Parser" "is responsible to parse the config files and translate them into Scala case classes."
                configObjects = group "Top-level Configuration Objects" {
                    actions = component "Actions" "define the transformation between DataObjects" "" "extendable"
                    dataObjects = component "DataObjects" "define the location and format of data" "" "extendable"
                    connections = component "Connections" "Some DataObjects require a Connection to access remote data" "" "extendable"
                }
                group "Concepts" {
                    dag = component "DAG" "Executes a set of Actions as a `Directed Acyclic Graph`, which is executed by the SDLB Job. Independent Actions are executed in parallel according to the `parallelism` cmd line option."
                    executionMode = component "Execution Mode" "are responsible to select the data to be processed by the SDLB Job" "" "extendable"
                    authMode = component "Authentication Mode" "are responsible to authenticate with external ressources" "" "extendable"
                    constraintsExpectations = component "Constraints and Expectations" "can be used to ensure Data Quality" "" "extendable"
                    secretProviders = component "Secret Providers" "are responsible to replace secret values in the configuration using different key stores." "" "extendable"
                    housekeepingMode = component "Housekeeping Mode" "is responsible to cleanup or archive outdated data" "" "extendable"
                }
                group "Modules" {
                    component "Azure" "providing helpers to access Azure ressources like LogAnalytics"
                    component "Delta Lake" "providing DeltaLakeTableDataObject"
                    component "Iceberg" "providing IcebergTableDataObject"
                    component "Kafka" "providing KafkaTopicDataObject"
                    component "Snowflake" "providing SnowflakeTableDataObject and Snowpark integration (Execution Engine)"
                }
                group "Customization Hooks" {
                    initPlugin = component "Init Plugin" "is a hook to add additional startup or shutdown logic, like dynamic log configuration." "" "extendable"
                    stateListener = component "State Listeners" "is a hook to recieve and distribute state updates, e.g. writing it to separate tables for reporting." "" "extendable"
                    // stateListener
                }
                actions -> dataObjects "read/write"
                dataObjects -> connections "use"
                dataObjects -> constraintsExpectations "validates"
                dataObjects -> housekeepingMode "applies"
                actions -> executionMode "applies"
                connections -> authMode "use"
                dag -> actions "executes"
                configParser -> secretProviders "uses"
            }
            configExporter = container "Config Exporter" "Helper command line tool to parse and export a given configuration" "" "helper"
            schemaExporter = container "Schema Exporter" "Helper command line tool to export DataObject schemas and statistics" "" "helper"
        }

        user -> sdlbJob "launches"
        user -> configExporter "launches"
        user -> schemaExporter "launches"
        configParser -> config "reads"
        dag -> state "uses"
        dataObjects -> data "reads/writes"
        configExporter -> config "reads"
        configExporter -> parsedConfig "write"
        schemaExporter -> config "reads"
        schemaExporter -> schemas "write"
    }

    views {
        container sdlb {
            include *
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

        theme default
    }

}