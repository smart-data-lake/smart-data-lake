# Code for Getting Started With Smart DataLake
## Introduction
This the code used in the Step-By-Step Walk Through Guide to get started with Smart Data Lake Builder (SDL).

There are multiple ways to run the example code.
If you are coming from the getting started guide, just follow "Run with Docker".

## Run SDL with Docker

This is the easiest way to run SDL, as it needs no installation except from a docker environment.

### Build Spark docker image

    docker build -t sdl-spark .

### Build getting-started artifact

    mkdir .mvnrepo
    docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package

or you can also use maven directly if you have Java SDK and Maven installed

    mvn package

### Run SDL with docker image

    docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network=spark sdl-spark:latest -c /mnt/config --feed-sel .*

## Run SDL with Maven

This needs a Java SDK and Maven installation.

1. Set the following environment variable: `HADOOP_HOME=/path/to/hadoop` (see https://github.com/smart-data-lake/smart-data-lake).
1. Change directory to project root.
1. Execute all feeds: `mvn clean verify`

Note: To execute a single example:
```
 mvn clean package exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath io.smartdatalake.app.LocalSmartDataLakeBuilder --feed-sel .* --config ${PWD}/config" -Dexec.workingdir="${PWD}/target"
```
(requires Maven 3.3.1 or later)

## Run SDL in IntelliJ (on Windows)

This needs an Intellij and Java SDK installation. Please make sure you have: 
- Java 8 SDK or Java 11 SDK 
- Scala Version 12.2. You need to install the Scala-Plugin with this exact version and DO NOT UPGRADE to Scala 3.

1. Load the project as a maven project: Right-click on pom.xml file -> add as Maven Project
2. Ensure all correct dependencies are loaded: Right-click on pom.xml file, Maven -> Reload Project
4. Configure and run the following run configuration in IntelliJ IDEA:
   - Main class: `io.smartdatalake.app.LocalSmartDataLakeBuilder`
   - Program arguments: `--feed-sel <regex-feedname-selector> --config $ProjectFileDir$/config`
   - Working directory: `/path/to/sdl-examples/target` or just `target`
   - Environment variables:
      - `HADOOP_HOME=/path/to/hadoop` (see https://github.com/smart-data-lake/smart-data-lake)

## Run Polynote and Metastore with docker-compose (part-2)

In part-2 you learn about Notebooks and Metastore to work with the data of this tutorial. 
You can start these services in a docker environment with docker-compose.
To update the project with these services `unzip part-2.additional-files.zip` in the projects root folder.

### Build before first start

    mkdir -p ./data/_metastore
    docker-compose build

### Start Polynote notebooks and Metastore database

    docker-compose up
