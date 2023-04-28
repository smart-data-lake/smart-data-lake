---
id: build
title: Build SDL
---

## Build from Source Codehlknsdfjkldsfjklfds
In the [getting started guide](../getting-started/setup.md) we used Docker to get you up to speed quickly.
If you take a closer look at the [Dockerfile](https://github.com/smart-data-lake/getting-started/blob/master/Dockerfile), 
you will see that we simply execute Apache Maven for you to build the jar file and configure an appropriate entrypoint for the container. 
 
In a real world project, you probably want more control over the build process, this page helps you in this case.
 
Smart Data Lake Builder is build using [Apache Maven](https://maven.apache.org/).
Here is an overview of the various versions at play:

### Build Dependencies
SDL Version 1.x
- *Spark 2.4*
- JDK 8 (Spark 2 doesn't support JDK 9 or higher)
- Scala 2.11 or 2.12
- Maven 3.0 (or higher)

SDL Version 2.x
- *Spark 3.x*
- JDK >= 8
- Scala 2.12 (Spark 3 doesn't support scala 2.11 anymore)
- Maven 3.0 (or higher)

:::tip
If you don't have strong reasons to still use Spark 2.X, you should use the latest version of Smart Data Lake Builder which comes with Spark 3.X.
:::

### Releases and snapshots
You rarely need to build Smart Data Lake Builder yourself. 
We publish releases regularly on [Github](https://github.com/smart-data-lake/smart-data-lake/releases).
These releases are automatically published on Maven Central and can therefore be used directly. 
On every merge to the develop branch, we also release snapshot releases to Sonatype, so you can even reference SNAPSHOT releases for cutting edge versions. 

### Start a new project
So how do you usually start with a new project?
Take a look at [sdl-examples](https://github.com/smart-data-lake/sdl-examples) as a template.
You start a new Maven project and define our `sdl-parent` as your projects parent:
```
<parent>
    <groupId>io.smartdatalake</groupId>
    <artifactId>sdl-parent</artifactId>
    <!--
        Set the smartdatalake version to use here.
        If version cannot be resolved, make sure maven central repository is defined in settings.xml and the corresponding profile activated.
        If version in IntelliJ still cannot be resolved, a restart of IntelliJ might help!
    -->
    <version>2.1.1</version>
</parent>
```

### Building JAR with Runtime Dependencies
With that, you also get all profiles defined in our parent project,
so it's easy to generate a ***fat-jar*** for example (including all dependencies you need). 
When deploying to a cluster with Apache Spark preconfigured, you don't need to include this dependency yourself. 
Use the profile ***fat-jar*** in this case.   
If you want to generate a jar for local execution or somewhere Apache Spark is not provided, use the profile ***fat-jar-with-spark*** instead

## Build an SDL Container

To build an SDL container a *Dockerfile* and a *pom.xml* is neccessary. The Dockerfile specifies:

* maven base image, and the openjdk image
* SDL specifiaction, defined in the `pom.xml`
* log4j property file
* entrypoint

An example would be:

```
#
# Build stage
#
FROM docker.io/maven:3.6.0-jdk-11-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn --quiet -f /home/app/pom.xml -Pcopy-libs package

#
# Package stage
# Note that *.jar is provided to the docker image through /mnt/lib and added to the class-path for SDL.
#
FROM docker.io/openjdk:11-jre-slim
COPY --from=build /home/app/target/lib/*.jar /opt/app/lib/
COPY --from=build /home/app/src/main/resources/log4j.properties /home/app/lib/
ENTRYPOINT ["java","-D${CONFIG_OVERWRITE}", "-Duser.dir=/mnt/data","-Dlog4j.configuration=file:/home/app/lib/log4j.properties","-cp","/opt/app/lib/*:/mnt/lib/*","io.smartdatalake.app.LocalSmartDataLakeBuilder"]
```

Custom Scala Classes for e.g. DataObjects and Transformers, can be build seperately and mounted into the container (into `/mnt/lib`). 

<Tabs groupId = "docker-podman-switch"
defaultValue="docker"
values={[
{label: 'Docker', value: 'docker'},
{label: 'Podman', value: 'podman'},
]}>
<TabItem value="docker">

```jsx
docker build -t sdl-spark .
```

</TabItem>
<TabItem value="podman">

```jsx
podman build -t sdl-spark .
```

</TabItem>
</Tabs>


An example including the log4j.properties is also provided in the [getting-started](https://github.com/smart-data-lake/getting-started.git). 
