# USE PYTHON 3.13
FROM python:3.13

# DEFINE LABELS
LABEL authors="Alberto Pérez Pérez"
LABEL version="1.0"
LABEL description="Python Flask API image for creating plots with Plotly based on AEMET open API data"

# SET ARGS
ARG EXPOSED_PORT
ARG STORAGE_PREFIX
ARG STORAGE_BASE_DIR

# SET WORK DIRECTORY
WORKDIR /AutoPlot

# INSTALL DEPENDENCIES FOR OPENCV
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6 -y

# COPY AND RUN REQUIREMENTS.TXT
COPY ./Deployment/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# COPY ALL PROJECT FILES
COPY ./App ./App/

# COPY RESOURCES COUNTRIES SHAPES
COPY ./Resources/ ./Resources/

# EXPOSE PORT FOR GUNICORN
EXPOSE ${EXPOSED_PORT}

# CREATE ENVIROMENT VARIABLES
ENV STORAGE_PREFIX=${STORAGE_PREFIX}
ENV STORAGE_BASE_DIR=${STORAGE_BASE_DIR}
ENV PYTHONPATH="/AutoPlot/"

WORKDIR /AutoPlot/App/

# RUN GUNICORN SERVER
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--timeout", "600", "--graceful-timeout", "600", "--access-logfile", "-", "--error-logfile", "-", "app:app"]

HEALTHCHECK --interval=5m --timeout=5s --start-period=10s \
    CMD curl -f http://localhost:8000/health || exit 1

