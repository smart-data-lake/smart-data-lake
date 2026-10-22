#
# Build stage
#
FROM docker.io/maven:3.6.0-jdk-11-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
#RUN mvn --quiet -f /home/app/pom.xml "-Dmaven.repo.local=/mnt/.mvnrepo" -Pcopy-libs package
RUN mvn -f /home/app/pom.xml -Dmaven.repo.local=/mnt/.mvnrepo -Pcopy-libs package

#
# Package stage
# Note that getting-started.jar is provided to the docker image through /mnt/lib and added to the class-path for SDL.
#
FROM docker.io/openjdk:11-jre-slim
COPY --from=build /home/app/target/lib/*.jar /opt/app/lib/
COPY --from=build /home/app/src/main/resources/log4j.properties /home/app/lib/

RUN apt-get update
RUN apt-get -y install buildah
RUN echo 'unqualified-search-registries=["docker.io"]' >> /etc/containers/registries.conf
RUN buildah --storage-driver=vfs from --name airbyte-mssql docker.io/airbyte/source-mssql

ENTRYPOINT ["java","-Duser.dir=/mnt/data","-Dlog4j.configuration=file:/home/app/lib/log4j.properties","-cp","/opt/app/lib/*:/mnt/lib/*","io.smartdatalake.app.LocalSmartDataLakeBuilder"]
