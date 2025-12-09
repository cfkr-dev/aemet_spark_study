@echo off
setlocal enabledelayedexpansion

:: ====== COLORS ======
for /f %%i in ('echo prompt $E^| cmd') do set "ESC=%%i"
set "C_INFO=%ESC%[36m"
set "C_OK=%ESC%[32m"
set "C_ERR=%ESC%[31m"
set "C_RST=%ESC%[0m"

:: ===== MAIN VARIABLES =====
set "CWD=%CD%"
set "DOCKERFILE_FILE=.\Dockerfile"
set "ENV_FILE=.\.env"
set "JAR_PATH=..\..\data-extraction-1.0.0.jar"
set "IMAGE_NAME=data-extraction:v1.0"

:: ===== EXIT FLAG =====
set "SCRIPT_FAILED=0"

:: =================
::     MAIN FLOW
:: =================
echo.
call :info "Checking required files"
call :check_file "%DOCKERFILE_FILE%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

call :check_file "%ENV_FILE%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Loading environment variables"
call :load_env_file "%ENV_FILE%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Checking Docker image"
call :docker_exists "!IMAGE_NAME!"
if ERRORLEVEL 1 (
    call :info "Docker image does not exist, will build it"
) else (
    call :ok "Docker image already exists"
    call :run_container
    if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end
    goto :end
)

echo.
call :info "Checking JAR file"
call :check_file "!JAR_PATH!"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Checking config directory"
if not defined HOST_STORAGE_PREFIX (
    call :err "HOST_STORAGE_PREFIX not defined"
    set SCRIPT_FAILED=1
    goto :end
)
if not defined HOST_STORAGE_BASE (
    call :err "HOST_STORAGE_BASE not defined"
    set SCRIPT_FAILED=1
    goto :end
)
call :check_file "!HOST_STORAGE_PREFIX!!HOST_STORAGE_BASE!\config"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Copying JAR"
call :copy "!JAR_PATH!" "!CWD!\app.jar"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Building Docker image"
docker build -t "!IMAGE_NAME!" -f "!DOCKERFILE_FILE!" .
if ERRORLEVEL 1 (
    call :err "Docker build failed"
    set SCRIPT_FAILED=1
    goto :cleanup
)

echo.
call :info "Waiting for Docker image to appear"
call :wait_for_docker_image "!IMAGE_NAME!" 5
if ERRORLEVEL 1 (
    call :err "Docker image not found after retries"
    set SCRIPT_FAILED=1
    goto :cleanup
)

call :ok "Docker build success"

:cleanup
echo.
call :info "Cleaning copied JAR"
call :remove "!CWD!\app.jar"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Running Docker container"
call :run_container
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

:end
echo.
if %SCRIPT_FAILED%==0 (
    echo %C_OK%[SUCCESS]%C_RST% Script finished successfully
    exit /b 0
) else (
    echo %C_ERR%[FAIL]%C_RST% Script terminated due to error
    exit /b 1
)

:: =================
::     FUNCTIONS
:: =================

:info
echo %C_INFO%[INFO]%C_RST% %~1
exit /b 0

:ok
echo %C_OK%[OK]%C_RST% %~1
exit /b 0

:err
echo %C_ERR%[ERROR]%C_RST% %~1
exit /b 1

:check_file
set "check_path=%~1"
set "check_path=!check_path:'=!"
set "check_path=!check_path:"=!"
if not exist "!check_path!" (
    call :err "Not found: %~1"
    exit /b 1
)
call :ok "%~1"
exit /b 0

:copy
set "src=%~1"
set "dst=%~2"
set "src=!src:'=!"
set "dst=!dst:'=!"
copy /Y "!src!" "!dst!" >nul 2>&1
if ERRORLEVEL 1 (
    call :err "Failed copying !src! to !dst!"
    exit /b 1
)
call :ok "Copied !src! to !dst!"
exit /b 0

:remove
del /Q "%~1" >nul 2>&1
if ERRORLEVEL 1 (
    call :err "Failed removing %~1"
    exit /b 1
)
call :ok "Removed %~1"
exit /b 0

:docker_exists
docker image inspect %~1 >nul 2>&1
exit /b %ERRORLEVEL%

:wait_for_docker_image
set "image=%~1"
set "retries=%~2"
:loop
call :docker_exists "%image%"
if ERRORLEVEL 0 exit /b 0
set /a retries-=1
if !retries! leq 0 exit /b 1
timeout /t 2 /nobreak >nul
goto loop

:load_env_file
for /f "usebackq tokens=* delims=" %%a in ("%~1") do (
    set "line=%%a"
    if defined line if "!line:~0,1!" neq "#" (
        for /f "tokens=1,* delims==" %%b in ("!line!") do (
            set "key=%%b"
            set "val=%%c"
            set "key=!key:'=!"
            set "val=!val:'=!"
            set "key=!key:"=!"
            set "val=!val:"=!"
            set "!key!=!val!"
            echo Set !key!=!val!
        )
    )
)
exit /b 0

:run_container
docker run --rm -it ^
 --env AEMET_OPENAPI_API_KEY="!AEMET_OPENAPI_API_KEY!" ^
 --env STORAGE_PREFIX="!CONTAINER_STORAGE_PREFIX!" ^
 --env STORAGE_BASE="!CONTAINER_STORAGE_BASE!" ^
 -v "!HOST_STORAGE_PREFIX!!HOST_STORAGE_BASE!":"!CONTAINER_STORAGE_PREFIX!!CONTAINER_STORAGE_BASE!" ^
 "!IMAGE_NAME!"
if ERRORLEVEL 1 exit /b 1
exit /b 0
