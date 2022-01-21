# Code for Getting Started With Smart DataLake
## Introduction
This the code used in the Step-By-Step Walk Through Guide to get started with Smart Data Lake Builder (SDL).

There are multiple ways to run the example code.
If you are coming from the getting started guide, 
just follow "Run with Docker".

## Run with Docker

### Build Spark docker image

    docker build -t sdl-gs-part-1 .

### Build getting-started artifact

    mvn package

or you can also use docker run if you have no java & maven installed

    mkdir .mvnrepo
    docker run -v ${PWD}:/mnt/project -v ${PWD}/.mvnrepo:/mnt/.mvnrepo maven:3.6.0-jdk-11-slim -- mvn -f /mnt/project/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" package

### Run SDL with docker image

    docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/target:/mnt/lib -v ${PWD}/config:/mnt/config --network=spark sdl-gs-part-1:latest -c /mnt/config --feed-sel compute.*

## Run SDL with Maven
1. Set the following environment variable: `HADOOP_HOME=/path/to/hadoop` (see https://github.com/smart-data-lake/smart-data-lake).
1. Change directory to project root.
1. Execute all feeds: `mvn clean verify`

Note: To execute a single example:
```
 mvn clean package exec:exec -Dexec.executable="java" -Dexec.args="-classpath %classpath io.smartdatalake.app.LocalSmartDataLakeBuilder --feed-sel <regex-pattern> --config <path-to-projectdir>/config" -Dexec.workingdir="target"
```
(requires Maven 3.3.1 or later)

## Run in IntelliJ (on Windows)
1. Ensure, that the directory `src/main/resources` is configured as a resource directory in IntelliJ (File - Project Structure - Modules).
1. Configure and run the following run configuration in IntelliJ IDEA:
   - Main class: `io.smartdatalake.app.LocalSmartDataLakeBuilder`
   - Program arguments: `--feed-sel <regex-feedname-selector> --config $ProjectFileDir$/config`
   - Working directory: `/path/to/sdl-examples/target` or just `target`
   - Environment variables:
      - `HADOOP_HOME=/path/to/hadoop` (see https://github.com/smart-data-lake/smart-data-lake)
