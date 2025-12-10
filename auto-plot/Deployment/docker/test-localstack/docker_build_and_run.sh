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
REQUIREMENTS_PATH="../../requirements.txt"
RESOURCES_DIR="../../../Resources"
APP_DIR="../../../App"
IMAGE_NAME="auto-plot:v1.0"

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

copy_dir() {
    local src="$1"
    local dst="$2"
    if ! cp -a "$src" "$dst" >/dev/null 2>&1; then
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

remove_dir() {
    local f="$1"
    if ! rm -rf "$f" >/dev/null 2>&1; then
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
        --env AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
        --env STORAGE_PREFIX="$CONTAINER_STORAGE_PREFIX" \
        --env STORAGE_BASE="$CONTAINER_STORAGE_BASE" \
        -p 8000:8000 \
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
info "Checking requirements file"
check_file "$REQUIREMENTS_PATH" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Checking resources directory"
check_file "$RESOURCES_DIR" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Checking app directory"
check_file "$APP_DIR" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Copying requirements"
copy_file "$REQUIREMENTS_PATH" "$CWD/requirements.txt" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Copying resources"
copy_file "$RESOURCES_DIR" "$CWD/Resources" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Copying app"
copy_file "$APP_DIR" "$CWD/App" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Building Docker image"
if ! docker build -t "$IMAGE_NAME" -f "$DOCKERFILE_FILE" .; then
    err "Docker build failed"
    SCRIPT_FAILED=1

    echo
    info "Cleaning copied requirements"
    remove_file "$CWD/requirements.txt" || true

    echo
    info "Cleaning copied resources"
    remove_file "$CWD/Resources" || true

    echo
    info "Cleaning copied app"
    remove_file "$CWD/App" || true

    exit $SCRIPT_FAILED
fi

echo
info "Waiting for Docker image to appear"
wait_for_docker_image "$IMAGE_NAME" 5 || {
    err "Docker image not found after retries"
    SCRIPT_FAILED=1

    echo
    info "Cleaning copied requirements"
    remove_file "$CWD/requirements.txt" || true

    echo
    info "Cleaning copied resources"
    remove_file "$CWD/Resources" || true

    echo
    info "Cleaning copied app"
    remove_file "$CWD/App" || true

    exit $SCRIPT_FAILED
}

ok "Docker build success"

echo
info "Cleaning copied requirements"
remove_file "$CWD/requirements.txt" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Cleaning copied resources"
remove_file "$CWD/Resources" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

echo
info "Cleaning copied app"
remove_file "$CWD/App" || { SCRIPT_FAILED=1; exit $SCRIPT_FAILED; }

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