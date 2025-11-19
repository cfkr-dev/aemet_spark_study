# USE PYTHON 3.13
FROM python:3.13

# DEFINE LABELS
LABEL authors="Alberto Pérez Pérez"
LABEL version="1.0"
LABEL description="Python Flask API image for creating plots with Plotly based on AEMET open API data"

# SET ARGS
ARG EXPOSED_PORT
ARG STORAGE_BASE_DIR
ARG STORAGE_RESOURCES_COUNTRIES_SHAPEFILE

# SET WORK DIRECTORY
WORKDIR /AutoPlot

# INSTALL DEPENDENCIES FOR OPENCV
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6 -y

# COPY AND RUN REQUIREMENTS.TXT
COPY ./Deployment/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# COPY ALL PROJECT FILES
COPY ./App ./App/

# EXPOSE PORT FOR GUNICORN
EXPOSE ${EXPOSED_PORT}

# CREATE ENVIROMENT VARIABLES
ENV STORAGE_BASE_DIR=${STORAGE_BASE_DIR}
ENV STORAGE_RESOURCES_COUNTRIES_SHAPEFILE=${STORAGE_RESOURCES_COUNTRIES_SHAPEFILE}
ENV PYTHONPATH="/AutoPlot/"

WORKDIR /AutoPlot/App/

# RUN GUNICORN SERVER
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--timeout", "600", "--graceful-timeout", "600", "--access-logfile", "-", "--error-logfile", "-", "app:app"]

