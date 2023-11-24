FROM maven:3.9.5 AS build

COPY src /home/app/src
COPY pom.xml /home/app
RUN mvn -f /home/app/pom.xml clean install -DskipTests

FROM openjdk:21
COPY --from=build /home/app/target/ods-metadata-0.0.1-SNAPSHOT.jar /usr/local/lib/ods-metadata-0.0.1-SNAPSHOT.jar
COPY boot.sh .

EXPOSE 8083
ENTRYPOINT ["bash","boot.sh"]
ENTRYPOINT ["java","-jar","/usr/local/lib/rods-metadata-0.0.1-SNAPSHOT.jar"]
