# USE JAVA 11
FROM eclipse-temurin:11-jre

# LABELS
LABEL authors="cfkr-dev"
LABEL version="1.0"
LABEL description="Spark application powered by Scala to perform queries on the meteorological dataset extracted from the AEMET and IFAPA APIs"

# WORKDIR
WORKDIR /SparkApp

# COPY JAR
COPY ./app.jar ./app.jar

# COPY ENTRYPOINT.SH
COPY ./entrypoint.sh ./entrypoint.sh

# EXPOSE PORT FOR SPARK STATS
EXPOSE 4040

# GIVE EXECUTION PERMISSIONS
RUN chmod +x entrypoint.sh

# INSTALL DEPENDENCIES
RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && ./aws/install && \
    rm -rf aws awscliv2.zip && \
    apt-get clean
