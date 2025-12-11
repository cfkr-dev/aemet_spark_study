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
set "AUTO_PLOT_WD=..\..\..\..\..\auto-plot\Deployment\docker\local"
set "DATA_EXTRACTION_WD=..\..\..\data-extraction\docker\local"
set "SPARK_APP_WD=..\..\..\spark-app\docker\local"
set "PLOT_GENERATION_WD=..\..\..\plot-generation\docker\local"
set "BUILD_SCRIPT=.\docker_build_and_run.bat"
set "COMPOSE_FILE=.\docker-compose.yaml"
set "ENV_FILE=.\.env"
set "AUTO_PLOT_IMAGE_NAME=auto-plot:v1.0"
set "DATA_EXTRACTION_IMAGE_NAME=data-extraction:v1.0"
set "SPARK_APP_IMAGE_NAME=spark-app:v1.0"
set "PLOT_GENERATION_IMAGE_NAME=plot-generation:v1.0"

:: ===== EXIT FLAG =====
set "SCRIPT_FAILED=0"

:: =================
::     MAIN FLOW
:: =================
echo.
call :info "Checking required files"
call :check_file "%COMPOSE_FILE%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

call :check_file "%ENV_FILE%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

cd /d "!AUTO_PLOT_WD!"
call :check_file "%BUILD_SCRIPT%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end
cd /d "!CWD!"

cd /d "!DATA_EXTRACTION_WD!"
call :check_file "%BUILD_SCRIPT%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end
cd /d "!CWD!"

cd /d "!SPARK_APP_WD!"
call :check_file "%BUILD_SCRIPT%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end
cd /d "!CWD!"

cd /d "!PLOT_GENERATION_WD!"
call :check_file "%BUILD_SCRIPT%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end
cd /d "!CWD!"

echo.
call :info "Loading environment variables"
call :load_env_file "%ENV_FILE%"
if ERRORLEVEL 1 set SCRIPT_FAILED=1 & goto :end

echo.
call :info "Checking auto-plot Docker image"
call :docker_exists "!AUTO_PLOT_IMAGE_NAME!"
if ERRORLEVEL 1 (
    call :info "auto-plot image does not exist, building it"
    cd /d "!AUTO_PLOT_WD!"
    call "!BUILD_SCRIPT!" --build-only
    if ERRORLEVEL 1 (
        cd /d "!CWD!"
        call :err "auto-plot build script failed"
        set SCRIPT_FAILED=1
        goto :end
    )
    cd /d "!CWD!"
) else (
    call :ok "auto-plot image already exists"
)

echo.
call :info "Checking data-extraction Docker image"
call :docker_exists "!DATA_EXTRACTION_IMAGE_NAME!"
if ERRORLEVEL 1 (
    call :info "data-extraction image does not exist, building it"
    cd /d "!DATA_EXTRACTION_WD!"
    call "!BUILD_SCRIPT!" --build-only
    if ERRORLEVEL 1 (
        cd /d "!CWD!"
        call :err "data-extraction build script failed"
        set SCRIPT_FAILED=1
        goto :end
    )
    cd /d "!CWD!"
) else (
    call :ok "data-extraction image already exists"
)

echo.
call :info "Checking spark-app Docker image"
call :docker_exists "!SPARK_APP_IMAGE_NAME!"
if ERRORLEVEL 1 (
    call :info "spark-app image does not exist, building it"
    cd /d "!SPARK_APP_WD!"
    call "!BUILD_SCRIPT!" --build-only
    if ERRORLEVEL 1 (
        cd /d "!CWD!"
        call :err "spark-app build script failed"
        set SCRIPT_FAILED=1
        goto :end
    )
    cd /d "!CWD!"
) else (
    call :ok "spark-app image already exists"
)

echo.
call :info "Checking plot-generation Docker image"
call :docker_exists "!PLOT_GENERATION_IMAGE_NAME!"
if ERRORLEVEL 1 (
    call :info "plot-generation image does not exist, building it"
    cd /d "!PLOT_GENERATION_WD!"
    call "!BUILD_SCRIPT!" --build-only
    if ERRORLEVEL 1 (
        cd /d "!CWD!"
        call :err "plot-generation build script failed"
        set SCRIPT_FAILED=1
        goto :end
    )
    cd /d "!CWD!"
) else (
    call :ok "plot-generation image already exists"
)

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
call :info "Starting Docker Compose"
call :run_compose
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

:docker_exists
docker image inspect %~1 >nul 2>&1
exit /b %ERRORLEVEL%

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

:run_compose
docker-compose -f "%COMPOSE_FILE%" up --no-deps --remove-orphans --abort-on-container-exit data-extraction
docker-compose -f "%COMPOSE_FILE%" up --no-deps --remove-orphans --abort-on-container-exit spark-app
start "AUTO_PLOT" cmd /c docker-compose -f "%COMPOSE_FILE%" up --no-deps --remove-orphans --force-recreate auto-plot
docker-compose -f "%COMPOSE_FILE%" up --no-deps --remove-orphans --abort-on-container-exit plot-generation
docker-compose -f "%COMPOSE_FILE%" down --volumes --remove-orphans

if ERRORLEVEL 1 exit /b 1
exit /b 0
