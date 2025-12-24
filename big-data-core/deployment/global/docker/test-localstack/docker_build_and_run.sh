#!/bin/bash
set -euo pipefail

# ====== COLORS ======
ESC="$(printf '\033')"
C_INFO="${ESC}[36m"
C_OK="${ESC}[32m"
C_ERR="${ESC}[31m"
C_RST="${ESC}[0m"

# ===== MAIN VARIABLES =====
CWD="$(pwd)"
AUTO_PLOT_WD="../../../../../auto-plot/Deployment/docker/test-localstack"
DATA_EXTRACTION_WD="../../../data-extraction/docker/test-localstack"
SPARK_APP_WD="../../../spark-app/docker/test-localstack"
PLOT_GENERATION_WD="../../../plot-generation/docker/test-localstack"
BUILD_SCRIPT="./docker_build_and_run.bat"
COMPOSE_FILE="./docker-compose.yaml"
ENV_FILE="./.env"
export AUTO_PLOT_IMAGE_NAME="auto-plot:v1.0"
export DATA_EXTRACTION_IMAGE_NAME="data-extraction-localstack:v1.0"
export SPARK_APP_IMAGE_NAME="spark-app-localstack:v1.0"
export PLOT_GENERATION_IMAGE_NAME="plot-generation-localstack:v1.0"

# ===== EXIT FLAG =====
SCRIPT_FAILED=0

# =================
#     FUNCTIONS
# =================
info() { echo -e "${C_INFO}[INFO]${C_RST} $1"; }
ok()   { echo -e "${C_OK}[OK]${C_RST} $1"; }
err()  { echo -e "${C_ERR}[ERROR]${C_RST} $1"; return 1; }

check_file() {
    local f="$1"
    if [[ ! -e "$f" ]]; then
        err "Not found: $f"
        return 1
    fi
    ok "$f"
}

docker_exists() {
    docker image inspect "$1" >/dev/null 2>&1
}

load_env_file() {
    while IFS= read -r line; do
        [[ -z "$line" || "$line" =~ ^# ]] && continue
        key="${line%%=*}"
        val="${line#*=}"

        key="${key//\'/}"
        key="${key//\"/}"
        val="${val//\'/}"
        val="${val//\"/}"

        export "$key"="$val"
        echo "Set $key=$val"

    done < "$1"

    return 0
}

run_compose() {
    gnome-terminal -- bash -c "docker compose -f \"$COMPOSE_FILE\" up --remove-orphans --force-recreate localstack" &
    docker compose -f "$COMPOSE_FILE" up --no-deps --remove-orphans --abort-on-container-exit data-extraction
    docker compose -f "$COMPOSE_FILE" up --no-deps --remove-orphans --abort-on-container-exit spark-app
    gnome-terminal -- bash -c "docker compose -f \"$COMPOSE_FILE\" up --no-deps --remove-orphans --force-recreate auto-plot" &
    docker compose -f "$COMPOSE_FILE" up --no-deps --remove-orphans --abort-on-container-exit plot-generation
    docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans

    return $?
}

# =================
#     MAIN FLOW
# =================
echo
info "Checking required files"
check_file "$COMPOSE_FILE" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

check_file "$ENV_FILE" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

cd "$AUTO_PLOT_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
check_file "$BUILD_SCRIPT" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

cd "$DATA_EXTRACTION_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
check_file "$BUILD_SCRIPT" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

cd "$SPARK_APP_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
check_file "$BUILD_SCRIPT" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

cd "$PLOT_GENERATION_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
check_file "$BUILD_SCRIPT" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Loading environment variables"
load_env_file "$ENV_FILE" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Checking auto-plot Docker image"
if docker_exists "$AUTO_PLOT_IMAGE_NAME"; then
    ok "auto-plot image already exists"
else
    info "auto-plot image does not exist, building it"

    cd "$AUTO_PLOT_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

    if ! "$BUILD_SCRIPT" --build-only; then
        cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
        err "auto-plot build script failed"
        SCRIPT_FAILED=1
        exit $SCRIPT_FAILED
    fi

    cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
fi

echo
info "Checking data-extraction Docker image"
if docker_exists "$DATA_EXTRACTION_IMAGE_NAME"; then
    ok "data-extraction image already exists"
else
    info "data-extraction image does not exist, building it"

    cd "$DATA_EXTRACTION_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

    if ! "$BUILD_SCRIPT" --build-only; then
        cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
        err "data-extraction build script failed"
        SCRIPT_FAILED=1
        exit $SCRIPT_FAILED
    fi

    cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
fi

echo
info "Checking spark-app Docker image"
if docker_exists "$SPARK_APP_IMAGE_NAME"; then
    ok "spark-app image already exists"
else
    info "spark-app image does not exist, building it"

    cd "$SPARK_APP_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

    if ! "$BUILD_SCRIPT" --build-only; then
        cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
        err "spark-app build script failed"
        SCRIPT_FAILED=1
        exit $SCRIPT_FAILED
    fi

    cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
fi

echo
info "Checking plot-generation Docker image"
if docker_exists "$PLOT_GENERATION_IMAGE_NAME"; then
    ok "plot-generation image already exists"
else
    info "plot-generation image does not exist, building it"

    cd "$PLOT_GENERATION_WD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

    if ! "$BUILD_SCRIPT" --build-only; then
        cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
        err "plot-generation build script failed"
        SCRIPT_FAILED=1
        exit $SCRIPT_FAILED
    fi

    cd "$CWD" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
fi

echo
info "Checking config directory"

if [[ -z "${HOST_STORAGE_PREFIX:-}" ]]; then
    err "HOST_STORAGE_PREFIX not defined"
    SCRIPT_FAILED=1
    exit $SCRIPT_FAILED
fi

if [[ -z "${HOST_STORAGE_BASE:-}" ]]; then
    err "HOST_STORAGE_BASE not defined"
    SCRIPT_FAILED=1
    exit $SCRIPT_FAILED
fi

check_file "${HOST_STORAGE_PREFIX}${HOST_STORAGE_BASE}/config" \
    || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Starting Docker Compose"
run_compose || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
if [[ $SCRIPT_FAILED -eq 0 ]]; then
    echo -e "${C_OK}[SUCCESS]${C_RST} Script finished successfully"
    exit 0
else
    echo -e "${C_ERR}[FAIL]${C_RST} Script terminated due to error"
    exit 1
fi
