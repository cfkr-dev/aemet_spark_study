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
REQUIREMENTS_PATH="../../requirements.txt"
RESOURCES_DIR="../../../Resources"
APP_DIR="../../../App"
IMAGE_NAME="auto-plot:v1.0"

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

copy_dir() {
    local src="$1"
    local dst="$2"
    cp -a "$src" "$dst" || err "Failed copying $src to $dst"
    ok "Copied $src to $dst"
}

remove_file() {
    local path="$1"
    rm -f "$path" || err "Failed removing $path"
    ok "Removed $path"
}

remove_dir() {
    local path="$1"
    rm -rf "$path" || err "Failed removing $path"
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
        --env AWS_S3_ENDPOINT="$AWS_S3_ENDPOINT" \
        --env STORAGE_PREFIX="$CONTAINER_STORAGE_PREFIX" \
        --env STORAGE_BASE="$CONTAINER_STORAGE_BASE" \
        -p 8000:8000 \
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
info "Checking requirements file"
check_file "$REQUIREMENTS_PATH"

echo
info "Checking resources directory"
check_file "$RESOURCES_DIR"

echo
info "Checking app directory"
check_file "$APP_DIR"

echo
info "Copying requirements"
copy_file "$REQUIREMENTS_PATH" "$CWD/requirements.txt"

echo
info "Copying resources"
copy_dir "$RESOURCES_DIR" "$CWD/Resources"

echo
info "Copying app"
copy_dir "$APP_DIR" "$CWD/App"

echo
info "Building Docker image"
docker build -t "$IMAGE_NAME" -f "$DOCKERFILE_FILE" . || err "Docker build failed"

echo
info "Waiting for Docker image to appear"
wait_for_docker_image "$IMAGE_NAME" 5 || err "Docker image not found after retries"

ok "Docker build success"

echo
info "Cleaning copied requirements"
remove_file "$CWD/requirements.txt"

echo
info "Cleaning copied resources"
remove_dir "$CWD/Resources"

echo
info "Cleaning copied app"
remove_dir "$CWD/App"

echo
info "Running Docker container"
run_container

ok "Script finished successfully"
exit 0