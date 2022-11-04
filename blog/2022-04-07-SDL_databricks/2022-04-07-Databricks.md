---
title: Deployment on Databricks
description: A brief example of deploying SDL on Databricks
slug: sdl-databricks
authors:
  - name: Mandes Schönherr
    title: Dr.sc.nat.
    url: https://github.com/mand35
tags: [Databricks, Cloud]
hide_table_of_contents: false
---

Many analytics applications are ported to the cloud, Data Lakes and Lakehouses in the cloud becoming more and more popular. 
The [Databricks](https://databricks.com) platform provides an easy accessible and easy configurable way to implement a modern analytics platform. 
Smart Data Lake Builder on the other hand provides an open source, portable automation tool to load and transform the data.

In this article the deployment of Smart Data Lake Builder (SDLB) on [Databricks](https://databricks.com) is described. 

<!--truncate-->

Before jumping in, it should be mentioned, that there are also many other methods to deploy SDLB in the cloud, e.g. using containers on Azure, Azure Kubernetes Service, Azure Synapse Clusters, Google Dataproc...
The present method provides the advantage of having many aspects taken care of by Databricks like Cluster management, Job scheduling and integrated data science notebooks.
Further, the presented SDLB pipeline is just a simple example, focusing on the integration into Databricks environment. 
SDLB provides a wide range of features and its full power is not revealed here. 

Let's get started:

1. [**Databricks**](https://databricks.com) accounts can be created as [Free Trial](https://databricks.com/try-databricks) or as [Community Account](https://community.databricks.com/s/login/SelfRegister)
    - Account and Workspace creation are described in detail [here](https://docs.databricks.com/getting-started/account-setup.html), there are few hints and modifications presented below.
    - I selected AWS backend, but there are conceptually no differences to the other providers. If you already have an Azure, AWS or Google Cloud account/subscription this can be used, otherwise you can register a trial subscription there. 
1. **Workspace stack** is created using the Quickstart as described in the documentation. When finished launch the Workspace.
1. **Databricks CLI**: for file transfer of configuration files, scripts and data, the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) is installed locally. **Configure** the CLI, using the Workspace URL and in the Workspace "Settings" -> "User Settings" -> "Access tokens" create a new token.
1. **Cluster** creation, in the Workspace open the *Cluster* Creation form.
    - Spark version: When selecting the *Databricks version* pay attention to the related Spark version. 
      This needs to match the Spark version we build SDLB with later. Here, `10.4 LTS` is selected with `Spark 3.2.1` and `Scala 2.12`. 
      Alternatively, SDLB can be build with a different Spark version, see also [Architecture](../../docs/architecture) for supported versions. 
    - typesafe library version correction script: the workspace currently includes version 1.2.1 from com.typesafe:config java library. 
      SDLB relies on functions of a newer version (>1.3.0) of this library. 
      Thus, we provide a newer version of the com.typesafe:config java library in an initialization script: *Advanced options* -> *Init Scripts* specify `dbfs:/databricks/scripts/config-install.sh`
        + Further, the script needs to be created and uploaded. You can use the following script in a local terminal:
        ```
        cat << EOF >> ./config-install.sh
        #!/bin/bash
        wget -O /databricks/jars/-----config-1.4.1.jar https://repo1.maven.org/maven2/com/typesafe/config/1.4.1/config-1.4.1.jar
        EOF
        databricks fs mkdirs dbfs:/databricks/scripts
        databricks fs cp ./config-install.sh dbfs:/databricks/scripts/
        ```
		
		Alternatively, you can also use a Databricks notebook for the script upload by executing the following cell:
		```
		%sh
		cat << EOF >> ./config-install.sh
		#!/bin/bash
		wget -O /databricks/jars/-----config-1.4.1.jar https://repo1.maven.org/maven2/com/typesafe/config/1.4.1/config-1.4.1.jar
		EOF
		mkdir /dbfs/databricks/scripts
		cp ./config-install.sh /dbfs/databricks/scripts/
		```

        Note: to double-check the library version I ran `grep typesafe pom.xml` in the [SmartDataLake](https://github.com/smart-data-lake/smart-data-lake.git) source

        Note: the added `-----` will ensure that this `.jar` is preferred before the default Workspace Spark version (which starts with `----`). 
        If you are curious you could double-check e.g. with a Workspace Shell Notebook running `ls /databricks/jars/*config*`

2. **fat-jar**:
       We need to provide the SDLB sources and all required libraries. Therefore, we compile and pack the Scala code into a Jar including the dependencies. We use the [getting-started](https://github.com/smart-data-lake/getting-started.git) as dummy project, which itself pulls the SDLB sources. 
    - download the [getting-started](https://github.com/smart-data-lake/getting-started.git) source and build it with the `-P fat-jar` profile
    ```
    podman run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -DskipTests  -P fat-jar  -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package
    ```
    General build instructions can be found in the [getting-started](../../docs/getting-started/setup#compile-scala-classes) documentation. 
    Therewith, the file `target/getting-started-1.0-jar-with-dependencies.jar` is created. 
    The *fat-jar* profile will include all required dependencies. The profile is defined in the [smart-data-lake](https://github.com/smart-data-lake/smart-data-lake) pom.xml.

3. upload files
	- JAR: in the "Workspace" -> your user -> create a directory `jars` and "import" the library using the link in "(To import a library, such as a jar or egg, click here)" and select the above created fat-jar to upload. As a result the jar will be listed in the Workspace directory. 
	- **SDLB application**: As an example a dataset from Airbnb NYC will be downloaded from Github, first written into a CSV file and later partially ported into a table. Therefore, the pipeline is defined first locally in a new file `application.conf`:
	```
	dataObjects {
	  ext-ab-csv-web {
	    type = WebserviceFileDataObject
	    url = "https://raw.githubusercontent.com/adishourya/Airbnb/master/new-york-city-airbnb-open-data/AB_NYC_2019.csv"
	    followRedirects = true
	    readTimeoutMs=200000
	  }
	  stg-ab {
	    type = CsvFileDataObject
	    schema = """id integer, name string, host_id integer, host_name string, neighbourhood_group string, neighbourhood string, latitude double, longitude double, room_type  string, price integer, minimum_nights integer, number_of_reviews integer, last_review timestamp, reviews_per_month double, calculated_host_listings_count integer,          availability_365 integer"""
	    path = "file:///dbfs/data/~{id}"
	  }
	  int-ab {
	    type = DeltaLakeTableDataObject
	    path = "~{id}"
	    table {
	      db = "default"
	      name = "int_ab"
	      primaryKey = [id]
	    }
	  }
	}

	actions {
	  loadWeb2Csv {
	    type = FileTransferAction
	    inputId = ext-ab-csv-web
	    outputId = stg-ab
	    metadata {
	      feed = download
	    }
	  }
	  loadCsvLoc2Db {
	    type = CopyAction
	    inputId = stg-ab
	    outputId = int-ab
	    transformers = [{
	      type = SQLDfTransformer
	      code = "select id, name, host_id,host_name,neighbourhood_group,neighbourhood,latitude,longitude from stg_ab"
	    }]
	    metadata {
	      feed = copy
	    }
	  }
	}
	```
	- upload using Databricks CLI 
	```
	databricks fs mkdirs dbfs:/conf/
	databricks fs cp application.conf dbfs:/conf/application.conf
	```

1. **Job creation**:
	Here, the Databricks job gets defined, specifying the SDL library and, the entry point and the arguments. Here we specify only the download feed. 
	Therefore, open in the sidebar *Jobs* -> *Create Job*: 
	- **Type**: `JAR`
	- **Main Class**: `io.smartdatalake.app.LocalSmartDataLakeBuilder`
	- **add** *Dependent Libraries*: "Workspace" -> select the file previously uploaded "getting-started..." file in the "jars" directory
	![jar select](add_library.png)
	- **Cluster** select the cluster created above with the corrected typesafe library
	- **Parameters**: `["-c", "file:///dbfs/conf/", "--feed-sel", "download"]`, which specifies the location of the SDLB configuration and selects the feed "download"
	![download task](download_task.png)

1. **Launch** the job: 
	Launch the job. 
	When finished in the "Runs" section of that job we can verify the successful run status

1. **Results**
    After running the SDLB pipeline the data should be downloaded into the staging file `stg_ab/result.csv` and selected parts into the table `int_ab`
    - csv file: in the first step we downloaded the CSV file. This can be verified, e.g. by inspecting the data directory in the Databricks CLI using `databricks fs ls dbfs:/data/stg-ab` or running in a Workspace shell notebook `ls /dbfs/data/stg-ab`
    - database: in the second phase specific columns are put into the database. This can be verified in the Workspace -> Data -> default -> int_ab
    ![select table](select_table.png)
    ![table](table.png)

    :::info
    Note that our final table was defined as `DeltaLakeTableDataObject`.
		With that, Smart Data Lake Builder automatically generates a Delta Lake Table in your Databricks workspace. 


## Lessons Learned
There are a few steps necessary, including building and uploading SDLB. 
Further, we need to be careful with the used versions of the underlying libraries. 
With these few steps we can reveal the power of SDLB and Databricks, creating a portable and reproducible pipeline into a Databricks Lakehouse.  
