#!/bin/sh

# ======================
#      COLORS
# ======================
ESC=$(printf '\033')
C_INFO="${ESC}[36m"
C_OK="${ESC}[32m"
C_ERR="${ESC}[31m"
C_RST="${ESC}[0m"

# ======================
#   MAIN VARIABLES
# ======================
CWD="$(pwd)"
DOCKERFILE_FILE="./Dockerfile"
COMPOSE_FILE="./docker-compose.yaml"
ENV_FILE="./.env"
ENTRYPOINT_SCRIPT="./entrypoint.sh"
JAR_PATH="../../data-extraction-1.0.0.jar"
IMAGE_NAME="data-extraction-localstack:v1.0"

SCRIPT_FAILED=0

# ======================
#    PRINT FUNCTIONS
# ======================
info() {  printf "%s[INFO]%s %s\n" "$C_INFO" "$C_RST" "$1"; }
ok()   {  printf "%s[OK]%s %s\n"   "$C_OK"  "$C_RST" "$1"; }
err()  {  printf "%s[ERROR]%s %s\n" "$C_ERR" "$C_RST" "$1"; }

# ======================
#    FILE CHECK
# ======================
check_file() {
  if [ ! -e "$1" ]; then
    err "Not found: $1"
    return 1
  fi
  ok "$1"
  return 0
}

# ======================
#     COPY FILE
# ======================
copy_file() {
  cp "$1" "$2" 2>/dev/null
  if [ $? -ne 0 ]; then
    err "Failed copying $1 to $2"
    return 1
  fi
  ok "Copied $1 to $2"
  return 0
}

# ======================
#     REMOVE FILE
# ======================
remove_file() {
  rm -f "$1" 2>/dev/null
  if [ $? -ne 0 ]; then
    err "Failed removing $1"
    return 1
  fi
  ok "Removed $1"
  return 0
}

# ======================
#   DOCKER IMAGE CHECK
# ======================
docker_exists() {
  docker image inspect "$1" >/dev/null 2>&1
  return $?
}

wait_for_docker_image() {
  IMAGE="$1"
  RETRIES="$2"

  while [ "$RETRIES" -gt 0 ]; do
    docker_exists "$IMAGE"
    if [ $? -eq 0 ]; then return 0; fi

    RETRIES=$((RETRIES - 1))
    sleep 2
  done
  return 1
}

# ======================
#     LOAD .ENV
# ======================
load_env_file() {
  file="$1"

  while IFS='=' read -r key value; do
    case "$key" in
      ''|\#*) continue ;;  # skip empty or comments
    esac

    key="$(echo "$key" | xargs)"
    value="$(echo "$value" | xargs)"

    eval "export $key=\"$value\""
    echo "Set $key=$value"
  done < "$file"

  return 0
}

# ======================
#   DOCKER-COMPOSE RUN
# ======================
run_compose() {
  gnome-terminal -- bash -c "docker-compose -f '$COMPOSE_FILE' up --remove-orphans --force-recreate localstack"
  docker-compose -f "$COMPOSE_FILE" up --no-deps --remove-orphans --abort-on-container-exit data-extraction
  docker-compose -f "$COMPOSE_FILE" down --volumes --remove-orphans
  return $?
}

# ======================
#        MAIN FLOW
# ======================

echo ""
info "Checking required files"
check_file "$DOCKERFILE_FILE" || SCRIPT_FAILED=1
check_file "$COMPOSE_FILE" || SCRIPT_FAILED=1
check_file "$ENV_FILE" || SCRIPT_FAILED=1
check_file "$ENTRYPOINT_SCRIPT" || SCRIPT_FAILED=1

[ $SCRIPT_FAILED -ne 0 ] && exit 1

echo ""
info "Loading environment variables"
load_env_file "$ENV_FILE" || SCRIPT_FAILED=1
[ $SCRIPT_FAILED -ne 0 ] && exit 1

echo ""
info "Checking Docker image"
docker_exists "$IMAGE_NAME"
if [ $? -eq 0 ]; then
  ok "Docker image already exists"
  run_compose || SCRIPT_FAILED=1
  exit $SCRIPT_FAILED
else
  info "Docker image does not exist, will build it"
fi

echo ""
info "Checking JAR file"
check_file "$JAR_PATH" || exit 1

echo ""
info "Checking config directory"

if [ -z "$HOST_STORAGE_PREFIX" ]; then err "HOST_STORAGE_PREFIX not defined"; exit 1; fi
if [ -z "$HOST_STORAGE_BASE" ]; then err "HOST_STORAGE_BASE not defined"; exit 1; fi

check_file "${HOST_STORAGE_PREFIX}${HOST_STORAGE_BASE}/config" || exit 1

echo ""
info "Copying JAR"
copy_file "$JAR_PATH" "$CWD/app.jar" || exit 1

echo ""
info "Building Docker image"
docker build -t "$IMAGE_NAME" -f "$DOCKERFILE_FILE" .
if [ $? -ne 0 ]; then
  err "Docker build failed"
  SCRIPT_FAILED=1
  remove_file "$CWD/app.jar"
  exit 1
fi

echo ""
info "Waiting for Docker image to appear"
wait_for_docker_image "$IMAGE_NAME" 5
if [ $? -ne 0 ]; then
  err "Docker image not found after retries"
  remove_file "$CWD/app.jar"
  exit 1
fi

ok "Docker build success"

echo ""
info "Cleaning copied JAR"
remove_file "$CWD/app.jar"

echo ""
info "Starting Docker Compose"
run_compose || SCRIPT_FAILED=1

# ======================
#           END
# ======================
echo ""
if [ $SCRIPT_FAILED -eq 0 ]; then
  printf "%s[SUCCESS]%s Script finished successfully\n" "$C_OK" "$C_RST"
  exit 0
else
  printf "%s[FAIL]%s Script terminated due to error\n" "$C_ERR" "$C_RST"
  exit 1
fi
