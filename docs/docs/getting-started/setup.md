---
id: setup
title: Technical Setup
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

:::info Trouble Shooting
In case you encounter an issue during this tutorial, feel free to consult the [trouble shooting section](troubleshooting/common-problems.md) of the Getting Started guide or the [one](../reference/troubleshooting.md) of the Smart Data Lake Reference. Another good source is the issue tracker on Github, either of the [Getting Started guide](https://github.com/smart-data-lake/getting-started/issues) or on the [main repository](https://github.com/smart-data-lake/smart-data-lake/issues).
:::

## Requirements

To run this tutorial you just need two things:

- [Podman](https://podman.io/get-started), a free Docker alternative on Linux. On Windows you might use it through WSL2, see also [Podman as an alternative to docker](troubleshooting/docker-on-windows.md).
- The [source code of the example](https://github.com/smart-data-lake/getting-started).

:::caution
Note for Windows Users (this includes WSL!). Deactivate the autocrlf function of git before cloning, otherwise it will break some podman scripts.
To do this, run `git config --global core.autocrlf false` in your terminal before cloning the repository.
:::

## Build Spark docker image

- Download the source code of the example either via git or by [downloading the zip](https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip) and extracting it.
- Open up a terminal and change to the folder with the source, you should see a file called Dockerfile in the Spark subfolder. 
- Run the following command from the root directory (note: this might take some time, but it's only needed once):

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

:::caution
The build reads the configuration files to generate the Scala interface, so it needs a configuration to be
in place. If you get an error stating that `dev.conf` or one of the `*.conf` files does not exist, seed the
working tree first with `./prepare.sh` as described in the next section, then rerun `./buildJob.sh`.
:::

## Prepare the configuration files

The repository does not contain the configuration files SDLB actually reads.
`config/` holds only `global.conf` and an empty `config.template`; the state of the configuration
after each step of this guide is tracked as a *variant* instead, e.g. `config/airports.conf.part-1-solution`.
A fresh clone can therefore not run anything before one of these variants is activated.

The `prepare.sh` script does that for you:

```
./prepare.sh --list                 # show the part -> file mapping
./prepare.sh 1                      # empty configs, the starting point of part 1
./prepare.sh 3 --clean              # starting point of part 3, and delete output of previous runs
./prepare.sh final                  # the completed pipeline of the whole guide
```

:::caution prepare.sh seeds the *start* of a part
`./prepare.sh 3` lays down the solution of part **2**, plus the files part 3 starts from - not the
part-3 solution. Use `./prepare.sh final` for the finished pipeline. `--list` is authoritative.
:::

Two options are useful as you work through the guide:
- `--clean` additionally deletes the tables and files written by previous runs. Files tracked by git are never touched.
- `--fix-timestamps` rewrites the time window of the flight data webservice in `config/departures.conf`, see [Get Departures](part-1/get-departures.md).

Each part of this guide starts by telling you which `prepare.sh` command to run, so you can also jump
straight into part 2 or part 3.

## Run SDLB with Spark docker image

Now let's see Smart Data Lake in action!

```
./prepare.sh 2
./startJob.sh --config /mnt/config,/mnt/envConfig/dev.conf --feed-sel download
```

`./prepare.sh 2` activates the solution of part 1 - the pipeline you are going to build yourself in the
next chapters.
This executes a simple data pipeline that downloads two files from two different websites into the *data* folder.

When the execution is complete, you should see the two new directories in the *data* folder.
Wonder what happened ? You will create the data pipeline that does just this in the first steps of this guide.

If you wish, you can start with [part 1](get-input-data) right away.
For [part 2](part-2/industrializing.md) and [part 3](part-3/custom-webservice.md), it is recommended to set up a Development Environment.

## Development Environment
For some parts of this tutorial it is beneficial to have a working development environment ready. In the following we will mainly explain how one can configure a working environment for 
Windows or Linux. We will focus on the community version of IntelliJ. Please [download](https://www.jetbrains.com/idea/) the version that suits your operating system.

### Hadoop Setup (Needed for Windows only)
Windows Users need to follow the steps below to have a working Hadoop Installation :
1. First download the Windows binaries for Hadoop [here](https://github.com/cdarlint/winutils/archive/refs/heads/master.zip)
2. Extract the wished version to a folder (e.g. \<prefix\>\hadoop-\<version\>\bin ). For this tutorial we use the version 3.2.2.
3. Configure the *HADOOP_HOME* environment variable to point to the folder \<prefix\>\hadoop-\<version\>
4. Add the *%HADOOP_HOME%\bin* to the *PATH* environment variable

### Run SDLB in IntelliJ
We will focus on the community version of IntelliJ. Please [download](https://www.jetbrains.com/idea/) the version that suits your operating system.
This needs an Intellij and Java SDK installation. Please make sure you have:
- Java 17 SDK (SDLB 3.x builds on Spark 4.x, which needs Java 17 or higher)
- Scala Version 2.13.
    - Install the Scala-Plugin (`File` -> `Settings` -> `Plugins`)
    - Install Scala version 2.13 and DO NOT UPGRADE to Scala 3. SDLB 3.x publishes `_2.13` artifacts only. For the complete list of versions at play in SDLB, [you can consult the Reference](../reference/build).
        - Existing Project: `File` -> `Project Structure` -> `Global Libraries` -> `Add` (Select correct version)
        - New Project: Select `Scala` under New Project and choose the correct version

Then do the following to load the project successfully:
1. Load the project as a maven project: Right-click on pom.xml file -> add as Maven Project
2. Ensure all correct dependencies are loaded: Right-click on pom.xml file, Maven -> Sync Project
3. Configure and run the following run configuration in IntelliJ IDEA (optional, as the .idea folder already contains this setup):
    - Main class: `io.smartdatalake.app.DefaultSmartDataLakeBuilder`
    - Program arguments: `-c $ProjectFileDir$/config --feed-sel <regex-feedname-selector> --state-path state -n getting-started`
    - Working directory: `$ProjectFileDir$/data`, so that relative paths of DataObjects resolve into the *data* folder just like in the container
    - VM Options: `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED`

**Congratulations!** You're now all setup! Head over to the next step to analyse these files...
