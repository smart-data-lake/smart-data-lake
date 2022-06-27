# This image contains
# - polynote notebook
# - a spark distribution (because polynote needs more than spark java artifacts)
# - SDL libraries (to use SDL in notebooks)

ARG POLYNOTE_VERSION=0.4.4
# check available versions at https://spark.apache.org/downloads.html
ARG SCALA_VERSION="2.12"

#
# Build sdl-lib stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY sdl-lib-pom.xml /home/app/
RUN mvn -f /home/app/sdl-lib-pom.xml package

#
# Package stage
#
FROM polynote/polynote:${POLYNOTE_VERSION}-${SCALA_VERSION}

# Arguments after `FROM` are reset
ARG SCALA_VERSION="2.12"
# check available versions at https://spark.apache.org/downloads.html
ARG SPARK_VERSION="3.2.1"
ARG HADOOP_VERSION="3.2"
# derby version used by this spark version - we need to load additional derbyclient jar to connect to remote metastore
ARG DERBY_VERSION="10.12.1.1"

USER root

WORKDIR /opt

# install spark distribution (polynote needs spark-submit command)
COPY install_spark.sh .
# convert line-endings to unix format and start script to install spark
RUN sed -i 's/\r$//' install_spark.sh && chmod +x install_spark.sh && ./install_spark.sh
ENV SPARK_HOME="/opt/spark"
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"

# copy sdl libraries
COPY --from=build /home/app/target/lib/*.jar $SPARK_HOME/jars/

# switch to non-root user
USER ${NB_USER}
