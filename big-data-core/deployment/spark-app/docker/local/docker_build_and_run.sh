#!/bin/bash
set -euo pipefail

# ====== COLORS ======
C_INFO="\033[36m"
C_OK="\033[32m"
C_ERR="\033[31m"
C_RST="\033[0m"

# ===== EXECUTION ARGS =====
BUILD_ONLY=false
if [[ "$1" == "--build-only" ]]; then
    BUILD_ONLY=true
fi

# ====== MAIN VARIABLES ======
CWD="$(pwd)"
DOCKERFILE_FILE="./Dockerfile"
ENV_FILE="./.env"
JAR_PATH="../../spark-app-1.0.0.jar"
IMAGE_NAME="spark-app:v1.0"

# ====== EXIT FLAG ======
SCRIPT_FAILED=0

# =======================
#        FUNCTIONS
# =======================
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

copy_file() {
    local src="$1"
    local dst="$2"
    if ! cp -f "$src" "$dst" >/dev/null 2>&1; then
        err "Failed copying $src to $dst"
        return 1
    fi
    ok "Copied $src to $dst"
}

remove_file() {
    local f="$1"
    if ! rm -f "$f" >/dev/null 2>&1; then
        err "Failed removing $f"
        return 1
    fi
    ok "Removed $f"
}

docker_exists() {
    docker image inspect "$1" >/dev/null 2>&1
}

wait_for_docker_image() {
    local image="$1"
    local retries="$2"
    while (( retries > 0 )); do
        if docker_exists "$image"; then return 0; fi
        sleep 2
        retries=$((retries-1))
    done
    return 1
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

run_container() {
    docker run --rm -it \
        -p 4040:4040 \
        --env STORAGE_PREFIX="$CONTAINER_STORAGE_PREFIX" \
        --env STORAGE_BASE="$CONTAINER_STORAGE_BASE" \
        --env RUNNING_IN_EMR="$RUNNING_IN_EMR" \
        -v "$HOST_STORAGE_PREFIX$HOST_STORAGE_BASE":"$CONTAINER_STORAGE_PREFIX$CONTAINER_STORAGE_BASE" \
        "$IMAGE_NAME"
}

# =======================
#        MAIN FLOW
# =======================
echo
info "Checking required files"
check_file "$DOCKERFILE_FILE" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
check_file "$ENV_FILE"        || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Loading environment variables"
load_env_file "$ENV_FILE" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Checking Docker image"
if docker_exists "$IMAGE_NAME"; then
    ok "Docker image already exists"

    if [[ "$BUILD_ONLY" == true ]]; then
        info "BUILD-ONLY active. Skipping execution"
        exit 0
    fi

    run_container || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }
    exit 0
else
    info "Docker image does not exist, will build it"
fi

echo
info "Checking JAR file"
check_file "$JAR_PATH" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

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
info "Copying JAR"
copy_file "$JAR_PATH" "$CWD/app.jar" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Building Docker image"
if ! docker build -t "$IMAGE_NAME" -f "$DOCKERFILE_FILE" .; then
    err "Docker build failed"
    SCRIPT_FAILED=1

    echo
    info "Cleaning copied JAR"
    remove_file "$CWD/app.jar" || true
    exit $SCRIPT_FAILED
fi

echo
info "Waiting for Docker image to appear"
wait_for_docker_image "$IMAGE_NAME" 5 || {
    err "Docker image not found after retries"
    SCRIPT_FAILED=1
    remove_file "$CWD/app.jar" || true
    exit $SCRIPT_FAILED
}

ok "Docker build success"

echo
info "Cleaning copied JAR"
remove_file "$CWD/app.jar" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

if [[ "$BUILD_ONLY" == true ]]; then
    info "BUILD-ONLY active. Skipping execution"
    exit 0
fi

echo
info "Running Docker container"
run_container || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
if [[ $SCRIPT_FAILED -eq 0 ]]; then
    echo -e "${C_OK}[SUCCESS]${C_RST} Script finished successfully"
    exit 0
else
    echo -e "${C_ERR}[FAIL]${C_RST} Script terminated due to error"
    exit 1
fi