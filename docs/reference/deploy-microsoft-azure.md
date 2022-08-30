---
id: deploy-microsoft-azure
title: Deploy on Microsoft Azure Databricks
---

Smart Data Lake Builder can be executed in multiple ways on Microsoft Azure:

* on Databricks
* as containers orchestrated with Kubernetes
* as virtual machine

## SDLB on Databricks
Databricks has the advantage of pre-configurated features like metastore, notebook support and integrated SQL endpoints.

At the time of this writing, a few extra steps are needed to overwrite specific libraries. 
When running a job in Databricks, a few dependencies are given and can not be simply overwritten with your own as described in the
[Azure documentation](https://docs.microsoft.com/en-us/azure/databricks/jobs#library-dependencies).
Since we use a newer version of typesafe config, we need to force the overwrite of this dependency.
We will create a cluster init script that downloads the library and saves it on the cluster, then use Sparks ChildFirstURLClassLoader to explicitly load our library first.
This can hopefully be simplified in the future.

1. In your Azure portal, create a Databricks Workspace and launch it
2. Create a cluster that fits your needs. For a first test you can use the miminal configuration of 1 Worker and 1 Driver node.
    This example was tested on Databricks Runtime Version 6.2.
3. Open the Advanced Options, Init Scripts and configure the path:
    `dbfs:/databricks/scripts/config-install.sh`
4. On your local machine, create a simple script called config-install.sh with the following content
    ```bash
    #!/bin/bash
    wget -O /databricks/jars/-----config-1.3.4.jar https://repo1.maven.org/maven2/com/typesafe/config/1.3.4/config-1.3.4.jar
    ```
5. To copy this local file to your Databricks filesystem, use the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html):
    ```bash
    databricks fs mkdirs dbfs:/databricks/scripts
    databricks fs cp \&ltpath-to/config-install.sh\&gt dbfs:/databricks/scripts/
    ```
    Now this script gets executed every time the cluster starts.
    It will download the config library and put it in a place where the classloader can find it.
6. Start your cluster, check the event log to see if it's up.
    If something is wrong with the init script, the cluster will not start.
7. On your local machine, create a second the SDLB configuration file(s) e.g. called application.conf. 
    For more details of the configuration file(s) see [hocon overview](hoconOverview.md). 
8. Upload the file(s) to a conf folder in dbfs:
    ```bash
    databricks fs mkdirs dbfs:/conf
    databricks fs cp path-to/application.conf dbfs:/conf/
    ```
9. Now create a Job with the following details:
     If you don't have the JAR file yet, see [build fat jar](build.md#building-jar-with-runtime-dependencies) on how to build it (using the Maven profile fat-jar).

     Task: Upload JAR - Choose the smartdatalake-&ltversion&gt-jar-with-dependencies.jar

     Main Class: io.smartdatalake.app.LocalSmartDataLakeBuilder
     Arguments: `["-c", "file:///dbfs/conf/", "--feed-sel", "download"]`

     The option *--override-jars* is set automatically to the correct value for DatabricksConfigurableApp.
     If you want to override any additional libraries, you can provide a list with this option.

10. Finally the job can be started and the result checked. 

For a detailed example see [Deployment on Databricks](../../blog/sdl-databricks) blog post.

<!-- TODO documentation about deployment on Kubernetes -->

<!-- TODO documentation about deployment as VM on Azure -->