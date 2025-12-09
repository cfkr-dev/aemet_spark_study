# USE JAVA 11
FROM eclipse-temurin:11-jre

# LABELS
LABEL authors="cfkr-dev"
LABEL version="1.0"
LABEL description="Scala application to make requests to the AEMET and IFAPA APIs to extract meteorological information"

# WORKDIR
WORKDIR /DataExtraction

# COPY JAR
COPY ./app.jar ./app.jar

# COPY ENTRYPOINT.SH
COPY ./entrypoint.sh ./entrypoint.sh

# GIVE EXECUTION PERMISSIONS
RUN chmod +x entrypoint.sh

# INSTALL DEPENDENCIES
RUN apt-get update && \
    apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && ./aws/install && \
    rm -rf aws awscliv2.zip && \
    apt-get clean
