# USE JAVA 11
FROM eclipse-temurin:11-jre

# DEFINE LABELS
LABEL authors="cfkr-dev"
LABEL version="1.0"
LABEL description="Scala application to make requests to auto-plot app to generate graphs with the data extracted from spark"

# SET WORK DIRECTORY
WORKDIR /PlotGeneration

# COPY JAR TO ./
COPY ./app.jar ./app.jar

# COPY ENTRYPOINT.SH
COPY ./entrypoint.sh ./entrypoint.sh

# GIVE EXECUTION PERMISSIONS
RUN chmod +x entrypoint.sh

# INSTALL DEPENDENCIES
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean
