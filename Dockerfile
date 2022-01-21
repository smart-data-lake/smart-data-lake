#
# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml -Pcopy-libs package

#
# Package stage
# Note that getting-started.jar is provided to the docker image through /mnt/lib and added to the class-path for SDL.
#
FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/lib/*.jar /opt/app/lib/
COPY --from=build /home/app/src/main/resources/log4j.properties /home/app/lib/
ENTRYPOINT ["java","-Duser.dir=/mnt/data","-Dlog4j.configuration=file:/home/app/lib/log4j.properties","-cp","/opt/app/lib/*:/mnt/lib/*","io.smartdatalake.app.LocalSmartDataLakeBuilder"]