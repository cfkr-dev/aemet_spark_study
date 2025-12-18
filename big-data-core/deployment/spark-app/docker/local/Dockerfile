# USE JAVA 11
FROM eclipse-temurin:11-jre

# DEFINE LABELS
LABEL authors="cfkr-dev"
LABEL version="1.0"
LABEL description="Spark application powered by Scala to perform queries on the meteorological dataset extracted from the AEMET and IFAPA APIs"

# SET WORK DIRECTORY
WORKDIR /SparkApp

# COPY JAR TO ./
COPY ./app.jar ./app.jar

# EXPOSE PORT FOR SPARK STATS
EXPOSE 4040

# SET ENTRYPOINT FOR JAR EXECUTION
ENTRYPOINT ["java", "-Xmx8g","-Xms4g", "-jar", "app.jar"]
