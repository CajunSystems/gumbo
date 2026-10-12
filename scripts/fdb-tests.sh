#!/usr/bin/env bash
#
# Runs the FoundationDB test suite against a real cluster, in Docker, the way CI does.
#
#   scripts/fdb-tests.sh                 # the FoundationDB*Test classes
#   scripts/fdb-tests.sh --keep          # leave the cluster running afterwards
#   scripts/fdb-tests.sh --keep -Dtest=FoundationDBMultiWriterTest   # extra args go to mvn
#
# Two containers: the foundationdb/foundationdb image as the server, in host networking mode
# so it advertises 127.0.0.1:4500, and a JDK+Maven image with the FoundationDB client library
# installed, sharing the server's network namespace so that address is right from inside it.
# That is the same shape as CI's `fdb` job (a service container plus the client package on the
# runner), which is why it is worth having: what passes here passes there.
#
# The tests run with GUMBO_REQUIRE_FDB=true, so an unreachable cluster is a failure, not a
# skip. This repository's ~/.m2 is mounted for the dependency cache, and the checkout is
# mounted as the working directory, so target/ is written by the container.
set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.43}"
SERVER="gumbo-fdb"
RUNNER_IMAGE="gumbo-fdb-runner:${FDB_VERSION}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

KEEP=false
MVN_ARGS=()
for arg in "$@"; do
  case "$arg" in
    --keep) KEEP=true ;;
    *) MVN_ARGS+=("$arg") ;;
  esac
done
if [ "${#MVN_ARGS[@]}" -eq 0 ]; then
  MVN_ARGS=(-Dtest='FoundationDB*Test')
fi

cleanup() {
  if [ "$KEEP" = false ]; then
    docker rm -f "$SERVER" >/dev/null 2>&1 || true
  else
    echo "Leaving $SERVER running (docker rm -f $SERVER to stop it)."
  fi
}
trap cleanup EXIT

# --- the client image: JDK 21 + Maven + libfdb_c, built once and cached by Docker ----------
if ! docker image inspect "$RUNNER_IMAGE" >/dev/null 2>&1; then
  echo "Building $RUNNER_IMAGE ..."
  docker build --platform linux/amd64 -t "$RUNNER_IMAGE" --build-arg "FDB_VERSION=$FDB_VERSION" - <<'EOF'
FROM eclipse-temurin:21-jdk-jammy
ARG FDB_VERSION
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates maven \
 && curl -fsSL "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" -o /tmp/fdb-clients.deb \
 && dpkg -i /tmp/fdb-clients.deb && rm /tmp/fdb-clients.deb \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /work
EOF
fi

# --- the server ------------------------------------------------------------------------------
if ! docker inspect -f '{{.State.Running}}' "$SERVER" 2>/dev/null | grep -q true; then
  docker rm -f "$SERVER" >/dev/null 2>&1 || true
  echo "Starting FoundationDB $FDB_VERSION ..."
  docker run -d --name "$SERVER" --platform linux/amd64 \
    -e FDB_NETWORKING_MODE=host -p 4500:4500 \
    "foundationdb/foundationdb:${FDB_VERSION}" >/dev/null
  # The image starts fdbserver but does not create a database; that is a one-off fdbcli call.
  for _ in $(seq 1 30); do
    if docker exec "$SERVER" fdbcli --exec 'configure new single memory' 2>/dev/null | grep -q 'Database created'; then
      break
    fi
    sleep 2
  done
fi
for _ in $(seq 1 30); do
  if docker exec "$SERVER" fdbcli --exec 'status minimal' 2>/dev/null | grep -q 'The database is available'; then
    break
  fi
  sleep 2
done
docker exec "$SERVER" fdbcli --exec 'status minimal'

# --- the tests, inside the client image, on the server's network namespace ------------------
docker run --rm --platform linux/amd64 --network "container:$SERVER" \
  -v "$REPO:/work" -v "$HOME/.m2:/root/.m2" \
  -e FDB_CLUSTER_FILE=/tmp/fdb.cluster -e GUMBO_REQUIRE_FDB=true \
  "$RUNNER_IMAGE" bash -c '
    echo "docker:docker@127.0.0.1:4500" > /tmp/fdb.cluster
    mvn -B --no-transfer-progress -Dsurefire.failIfNoSpecifiedTests=false test "$@"
    echo "--- surefire ---"
    grep -h "Tests run" target/surefire-reports/*FoundationDB*.txt
  ' _ "${MVN_ARGS[@]}"
