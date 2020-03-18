# Run on Microsoft Azure Databricks

Smart Data Lake can be executed on the Databricks Service running on Microsoft Azure. 
The following steps show how to execute a simple copy feed. 

At the time of this writing, a few extra steps are needed to overwrite specific libraries. 
When running a job in Databricks, a few dependencies are given and can not be simply overwritten with your own as described in the 
[Azure documentation](https://docs.microsoft.com/en-us/azure/databricks/jobs#library-dependencies).
Since we use a newer version of typesafe config, we need to force the overwrite of this dependency. 
We will create a cluster init script that downloads the library and saves it on the cluster, then use Sparks ChildFirstURLClassLoader to explicitly load our library first.
This can hopefully be simplified in the future.

1.  In your Azure portal, create a Databricks Workspace and launch it
1.  Create a cluster as you see fit, for a first test you can use the miminal configuration of 1 Worker and 1 Driver node. 
    This example was tested on Databricks Runtime Version 6.2.
1.  Open the Advanced Options, Init Scripts and configure the path:
    dbfs:/databricks/scripts/config-install.sh
1.  On your local machine, create a simple script called config-install.sh with the following content
    ```bash
    #!/bin/bash
    wget -O /databricks/jars/config-1.3.4.jar https://repo1.maven.org/maven2/com/typesafe/config/1.3.4/config-1.3.4.jar
    ```
1.  To copy this local file to your Databricks filesystem, use the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html):
    ```bash
    databricks fs mkdirs dbfs:/databricks/scripts
    databricks fs cp <path-to/config-install.sh> dbfs:/databricks/scripts/
    ```
    Now this script gets executed everytime the cluster starts.
    It will download the config library and put it in a place where the classloader can find it.
1.  Start your cluster, check the event log to see if it's up. 
    If something is wrong with the init script, the cluster will not start.
1.  On your local machine, create a second file called application.conf with the following content:
    ```hocon   
    connections {}
    
    dataObjects {
     
      ab-csv-dbfs {
        type = CsvFileDataObject
        path = "file:///dbfs/data/AB_NYC_2019.csv"
        csv-options {
          delimiter = ","
          escape = "\\"
          header = "true"
          quote = "\""
        }
      }
    
      ab-reduced-csv-dbfs {
        type = CsvFileDataObject
        path = "file:///dbfs/data/~{id}/nyc_reduced.csv"
        csv-options = {
          delimiter = ","
          escape = "\\"
          header = "true"
          quote = "\""
        }
      }
   
    }
    
    actions {
    
      loadDbfs2Dbfs {
        type = CopyAction
        inputId = ab-csv-dbfs
        outputId = ab-reduced-csv-dbfs
        metadata {
          feed = ab-azure
        }
      }
    
    }
    ```
1.  Upload the file to the conf folder in dbfs:
    ```bash
    databricks fs mkdirs dbfs:/conf
    databricks fs cp <path-to/application.conf> dbfs:/conf/
    ```
1.  Also copy the example CSV file from sdl-examples to the data folder:
    ```bash
    databricks fs mkdirs dbfs:/data
    databricks fs cp <path-to/AB_NYC_2019.csv> dbfs:/data/
    ```
1.  Now create a Job with the following details:<br/>
    Task: Upload JAR - Choose the smartdatalake-\<version>-jar-with-dependencies.jar<br/>
    Main Class: io.smartdatalake.app.DatabricksSmartDataLakeBuilder
    Arguments: -c file:///dbfs/conf/ --feed-sel ab-azure --name azure -m yarn<br/>
    The option *--override-jars* is set automatically to the correct value for DatabricksConfigurableApp. 
    If you want to override any additional libraries, you can provide a list with this option. 
    
    If you don't have the JAR file yet, check the README on how to build it (using the Maven profile fat-jar). 
1.  Start the job and check the result, you should now have the output written in dbfs:/data/ab-reduced-csv-dbfs
