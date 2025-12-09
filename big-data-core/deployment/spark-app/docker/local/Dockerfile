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

# Comando por defecto para ejecutar la app
ENTRYPOINT ["java", "-jar", "app.jar"]
