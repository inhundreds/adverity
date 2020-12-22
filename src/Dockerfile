FROM adoptopenjdk/openjdk11:jre-11.0.6_10-alpine
ARG JAR_FILE=target/*.jar
COPY ${JAR_FILE} app.jar
COPY src/main/resources/data.csv /src/main/resources/data.csv
COPY src/main/resources/sample.csv /src/main/resources/sample.csv
ENTRYPOINT ["java","-jar","/app.jar"]