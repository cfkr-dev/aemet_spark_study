# USE JAVA 11
FROM eclipse-temurin:11-jre

# DEFINE LABELS
LABEL authors="cfkr-dev"
LABEL version="1.0"
LABEL description="Scala application to make requests to the AEMET and IFAPA APIs to extract meteorological information"

# SET WORK DIRECTORY
WORKDIR /DataExtraction

# COPY JAR TO ./
COPY ./app.jar ./app.jar

# SET ENTRYPOINT FOR JAR EXECUTION
ENTRYPOINT ["java", "-jar", "app.jar"]
