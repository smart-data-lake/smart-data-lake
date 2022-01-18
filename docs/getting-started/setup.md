---
id: setup
title: Technical Setup
---

## Requirements

To run this tutorial you just need two things:

- [Docker](https://www.docker.com/get-started), including docker-compose. If you use Windows, please read our note on [Docker for Windows](troubleshooting/docker-on-windows.md),
- The [source code of the example](https://github.com/smart-data-lake/getting-started).

## Build docker image

- Download the source code of the example either via git or by [downloading the zip](https://github.com/smart-data-lake/getting-started/archive/refs/heads/master.zip) and extracting it.
- Open up a terminal and change to the folder with the source, you should see a file called Dockerfile.
- Then run:


    docker build -t smart-data-lake/gs1 .


## Run docker image

Let's see Smart Data Lake in action!
Run the following commands in the same terminal:

    mkdir -f data
    docker run --rm -v ${PWD}/data:/mnt/data -v ${PWD}/config:/mnt/config smart-data-lake/gs1:latest --config /mnt/config --feed-sel download

This creates a folder in the current directory named *data* and then 
executes a simple data pipeline that downloads two files from two different websites into that directory.

When the execution is complete, you should see the two files in the *data* folder.
Wonder what happened ? You will create the data pipeline that does just this in the first steps of this guide.

## Development Environment
For some parts of this tutorial it is beneficial to have a working development environment ready. In the following we will mainly explain how one can configure a working evironment for 
Windows or Linux. We will focus on the community version of intellij. Please [download](https://www.jetbrains.com/idea/) the version that suits your operating system. 
### Windows
#### System setup 
1. First download the Windows binaries for Hadoop [here](https://github.com/cdarlint/winutils/archive/refs/heads/master.zip)
2. Extract the wished version to a folder (e.g. &lt prefix &gt\hadoop-&lt version &gt\bin ). For this tutorial we use the version 3.2.2.
3. Configure the *HADOOP_HOME* environment variable to point to the folder &lt prefix &gt\hadoop-&lt version &gt\
4. Add the *%HADOOP_HOME%\bin* to the *PATH* environment variable
   
#### Intellij
1. Open the project in intellj
2. Load the Maven project
3. Install the scala plugin
4. Select one of the preconfigured runConfigurations (part-x)
5. Run

### Linux
**TODO**

### Dockerized
**TODO**

**Congratulations!** You're now all setup! Head over to the next step to analyse these files...
