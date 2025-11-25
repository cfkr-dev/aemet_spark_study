@echo off
setlocal enabledelayedexpansion

REM change working directory to the root of the project
cd ..\..\..\

REM Define variables for the files
set DOCKERFILE_FILE=.\Deployment\Docker\Local\Dockerfile
set ENV_FILE=.\Deployment\Docker\Local\.env

REM Check if Dockerfile file exists
if not exist "%DOCKERFILE_FILE%" (
    echo File %DOCKERFILE_FILE% not found.
    exit /b 1
)

REM Check if .env file exists
if not exist "%ENV_FILE%" (
    echo File %ENV_FILE% not found.
    exit /b 1
)

REM Read .env file and define variables
for /f "usebackq tokens=* delims=" %%a in ("%ENV_FILE%") do (
    set "line=%%a"
    if not "!line!"=="" if "!line:~0,1!" neq "#" (
        for /f "tokens=1,* delims==" %%b in ("!line!") do (
            set "%%b=%%c"
        )
    )
)

REM print the variables with the .env content
echo HOST_EXPOSED_PORT = %HOST_EXPOSED_PORT%
echo CONTAINER_EXPOSED_PORT = %CONTAINER_EXPOSED_PORT%

echo HOST_STORAGE_PREFIX = %HOST_STORAGE_PREFIX%
echo CONTAINER_STORAGE_PREFIX = %CONTAINER_STORAGE_PREFIX%

echo STORAGE_BASE_DIR = %STORAGE_BASE_DIR%

REM set the docker image name
set IMAGE_NAME=autoplot:v1.0

REM build docker image
echo =======================
echo  BUILDING DOCKER IMAGE
echo =======================

docker build -t %IMAGE_NAME% -f "%DOCKERFILE_FILE%" .

REM check docker image build
if %ERRORLEVEL% neq 0 (
    echo DOCKER BUILD ERROR
    exit /b %ERRORLEVEL%
)

REM run docker image
echo.
echo ==========================
echo  RUNNING DOCKER CONTAINER
echo ==========================

docker run --rm -it ^
 --env EXPOSED_PORT=%CONTAINER_EXPOSED_PORT% ^
 --env STORAGE_PREFIX="%CONTAINER_STORAGE_PREFIX%" ^
 --env STORAGE_BASE_DIR="%STORAGE_BASE_DIR%" ^
 -p %HOST_EXPOSED_PORT%:%CONTAINER_EXPOSED_PORT% ^
 -v "%HOST_STORAGE_PREFIX%%STORAGE_BASE_DIR%":"%CONTAINER_STORAGE_PREFIX%%STORAGE_BASE_DIR%" ^
 %IMAGE_NAME%

endlocal