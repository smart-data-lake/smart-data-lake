#
# Build stage
#
FROM maven:3.6.0-jdk-11-slim AS build
COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml -Pfat-jar-with-spark package

#
# Package stage
#
FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/app-template-1.0-jar-with-dependencies.jar /usr/local/lib/demo.jar
ENTRYPOINT ["java","-Duser.dir=/mnt/data", "-jar","/usr/local/lib/demo.jar"]