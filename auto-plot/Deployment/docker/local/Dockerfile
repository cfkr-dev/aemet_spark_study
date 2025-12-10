# USE PYTHON 3.13
FROM python:3.13

# DEFINE LABELS
LABEL authors="Alberto Pérez Pérez"
LABEL version="1.0"
LABEL description="A Flask application powered by Python running on a Gunicorn server that creates graphs with Plotly using meteorological data"

# SET WORK DIRECTORY
WORKDIR /AutoPlot

# INSTALL DEPENDENCIES FOR OPENCV
RUN apt-get update && apt-get install ffmpeg libsm6 libxext6 -y

# COPY AND RUN REQUIREMENTS.TXT
COPY ./requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# COPY ALL PROJECT FILES
COPY ./App ./App/

# COPY RESOURCES COUNTRIES SHAPES
COPY ./Resources/ ./Resources/

# EXPOSE PORT FOR GUNICORN
EXPOSE 8000

# CREATE ENVIROMENT VARIABLES
ENV PYTHONPATH="/AutoPlot/"

WORKDIR /AutoPlot/App/

# RUN GUNICORN SERVER
CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "--timeout", "600", "--graceful-timeout", "600", "--access-logfile", "-", "--error-logfile", "-", "app:app"]

HEALTHCHECK --interval=5m --timeout=5s --start-period=10s \
    CMD curl -f http://localhost:8000/health || exit 1

