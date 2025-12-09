#!/bin/bash
set -e
set -o pipefail

# ===== COLORS =====
C_INFO="\033[36m"
C_OK="\033[32m"
C_ERR="\033[31m"
C_RST="\033[0m"

# ===== MAIN VARIABLES =====
CWD="$(pwd)"
DOCKERFILE_FILE="./Dockerfile"
ENV_FILE="./.env"
JAR_PATH="../../data-extraction-1.0.0.jar"
IMAGE_NAME="data-extraction:v1.0"

# ===== FUNCTIONS =====

info() {
    echo -e "${C_INFO}[INFO]${C_RST} $1"
}

ok() {
    echo -e "${C_OK}[OK]${C_RST} $1"
}

err() {
    echo -e "${C_ERR}[ERROR]${C_RST} $1"
    exit 1
}

check_file() {
    local path="$1"
    if [[ ! -e "$path" ]]; then
        err "Not found: $path"
    fi
    ok "$path"
}

copy_file() {
    local src="$1"
    local dst="$2"
    cp -f "$src" "$dst" || err "Failed copying $src to $dst"
    ok "Copied $src to $dst"
}

remove_file() {
    local path="$1"
    rm -f "$path" || err "Failed removing $path"
    ok "Removed $path"
}

docker_exists() {
    docker image inspect "$1" > /dev/null 2>&1
}

wait_for_docker_image() {
    local image="$1"
    local retries="$2"
    local count=0
    while ! docker_exists "$image"; do
        count=$((count+1))
        if [ "$count" -ge "$retries" ]; then
            return 1
        fi
        sleep 2
    done
    return 0
}

load_env_file() {
    local env_file="$1"
    if [[ ! -f "$env_file" ]]; then
        err "Env file not found: $env_file"
    fi

    while IFS= read -r line || [[ -n "$line" ]]; do
        # ignore empty lines or comments
        [[ -z "$line" || "${line:0:1}" == "#" ]] && continue
        # split on first '='
        key="${line%%=*}"
        val="${line#*=}"
        # remove quotes
        key="${key//\"/}"
        key="${key//\'/}"
        val="${val//\"/}"
        val="${val//\'/}"
        export "$key=$val"
        echo "Set $key=$val"
    done < "$env_file"
}

run_container() {
    docker run --rm -it \
        --env AEMET_OPENAPI_API_KEY="$AEMET_OPENAPI_API_KEY" \
        --env STORAGE_PREFIX="$CONTAINER_STORAGE_PREFIX" \
        --env STORAGE_BASE="$CONTAINER_STORAGE_BASE" \
        -v "$HOST_STORAGE_PREFIX$HOST_STORAGE_BASE":"$CONTAINER_STORAGE_PREFIX$CONTAINER_STORAGE_BASE" \
        "$IMAGE_NAME"
}

# ===== MAIN FLOW =====

echo
info "Checking required files"
check_file "$DOCKERFILE_FILE"
check_file "$ENV_FILE"

echo
info "Loading environment variables"
load_env_file "$ENV_FILE"

echo
info "Checking Docker image"
if docker_exists "$IMAGE_NAME"; then
    ok "Docker image already exists"
    run_container
    exit 0
else
    info "Docker image does not exist, will build it"
fi

echo
info "Checking JAR file"
check_file "$JAR_PATH"

echo
info "Checking config directory"
if [[ -z "$HOST_STORAGE_PREFIX" ]]; then
    err "HOST_STORAGE_PREFIX not defined"
fi
if [[ -z "$HOST_STORAGE_BASE" ]]; then
    err "HOST_STORAGE_BASE not defined"
fi
check_file "$HOST_STORAGE_PREFIX$HOST_STORAGE_BASE/config"

echo
info "Copying JAR"
copy_file "$JAR_PATH" "$CWD/app.jar"

echo
info "Building Docker image"
docker build -t "$IMAGE_NAME" -f "$DOCKERFILE_FILE" . || err "Docker build failed"

echo
info "Waiting for Docker image to appear"
wait_for_docker_image "$IMAGE_NAME" 5 || err "Docker image not found after retries"

ok "Docker build success"

echo
info "Cleaning copied JAR"
remove_file "$CWD/app.jar"

echo
info "Running Docker container"
run_container

ok "Script finished successfully"
exit 0
