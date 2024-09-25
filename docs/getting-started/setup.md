---
id: setup
title: Technical Setup
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Requirements

To run this tutorial you just need two things:

- [Podman](https://podman.io/get-started), a free Docker alternative on Linux. On Windows you might use it through WSL2, see also [podman as an alternative to docker](troubleshooting/docker-on-windows.md).
- The [source code of the example](https://github.com/smart-data-lake/getting-started).

## Build Spark docker image

- Download the source code of the example either via git or by [downloading the zip](https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip) and extracting it.
- Open up a terminal and change to the folder with the source, you should see a file called Dockerfile. 
- Then run (note: this might take some time, but it's only needed once):

```
./buildSpark.sh
```

:::note
This creates a docker image including Spark, Python and SDLB libraries according to the SDLB version configured in pom.xml as parent.version. 
:::

## Compile Scala Classes

Utilizing a Maven container, the getting-started project with the required SDLB Scala sources and all required libraries are compiled and packed using the following command:  

```
./buildJob.sh
```

:::note
This might take some time, but it's only needed at the beginning or if Scala code has changed.
:::

## Run SDLB with Spark docker image

Now let's see Smart Data Lake in action!

```
pushd config && cp departures.conf.part-1-solution departures.conf && cp airports.conf.part-1-solution airports.conf && cp btl.conf.part-1-solution btl.conf && popd
./startJob.sh --config /mnt/config,/mnt/envConfig/lab.conf --feed-sel download
```

This executes a simple data pipeline that downloads two files from two different websites into the *data* folder.

When the execution is complete, you should see the two new directories in the *data* folder.
Wonder what happened ? You will create the data pipeline that does just this in the first steps of this guide.

If you wish, you can start with [part 1](get-input-data) right away.
For [part 2](part-2/industrializing.md) and [part 3](part-3/custom-webservice.md), it is recommended to setup a Development Environment.

## Development Environment
For some parts of this tutorial it is beneficial to have a working development environment ready. In the following we will mainly explain how one can configure a working evironment for 
Windows or Linux. We will focus on the community version of Intellij. Please [download](https://www.jetbrains.com/idea/) the version that suits your operating system. 
### Hadoop Setup (Needed for Windows only)
Windows Users need to follow the steps below to have a working Hadoop Installation :
1. First download the Windows binaries for Hadoop [here](https://github.com/cdarlint/winutils/archive/refs/heads/master.zip)
2. Extract the wished version to a folder (e.g. &lt prefix &gt\hadoop-&lt version &gt\bin ). For this tutorial we use the version 3.2.2.
3. Configure the *HADOOP_HOME* environment variable to point to the folder &lt prefix &gt\hadoop-&lt version &gt\
4. Add the *%HADOOP_HOME%\bin* to the *PATH* environment variable

### Run SDLB in IntelliJ
We will focus on the community version of Intellij. Please [download](https://www.jetbrains.com/idea/) the version that suits your operating system.
This needs an Intellij and Java SDK installation. Please make sure you have:
- Java Java 17 SDK or Java 11 JDK
- Scala Version 2.12. You need to install the Scala-Plugin with this exact version and DO NOT UPGRADE to Scala 3. For the complete list of versions at play in SDLB, [you can consult the Reference](../reference/build).

1. Load the project as a maven project: Right-click on pom.xml file -> add as Maven Project
2. Ensure all correct dependencies are loaded: Right-click on pom.xml file, Maven -> Reload Project
3. Configure and run the following run configuration in IntelliJ IDEA:
    - Main class: `io.smartdatalake.app.LocalSmartDataLakeBuilder`
    - Program arguments: `--feed-sel <regex-feedname-selector> --config $ProjectFileDir$/config`
    - Working directory: `/path/to/sdl-examples/target` or just `target`
    - VM Options: `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED`

**Congratulations!** You're now all setup! Head over to the next step to analyse these files...
