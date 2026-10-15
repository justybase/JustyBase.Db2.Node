#!/usr/bin/env bash
# Local Db2 integration tests for JustyBase Db2 for Node.js.
#
# Starts a throwaway IBM Db2 Community container, waits until it really accepts
# SQL, runs the live integration tests against THAT container only, and removes
# the container (and its volumes) afterwards, also on failure or Ctrl-C.
#
# Usage: scripts/test-db2-integration.sh [--full] [--parity]
#   (default)  small smoke suite: connect, DML/types/NULLs, parameters,
#              transactions, result shape, error propagation
#   --full     every live test of this repository, including the parity oracle
#              (takes several minutes)
#   --parity   smoke suite plus differential SELECT/type parity against the
#              official node-ibm_db provider (test-only oracle, never packaged;
#              installs pinned ibm_db with --no-save and sets DB2_PARITY_IBMDB=1)
#   --soak     resource soak of the TypeScript driver (benchmarks/resource-soak.cjs)
#              after its warmup: row/LOB/procedure cycles, FD, socket, spool and RSS
#              checks. Duration: DB2_IT_SOAK_SECONDS (default 900)
#
# Every run writes a junit report and fails when a test is skipped without an
# allowed reason (tools/ci/check-junit-no-skips.cjs).
#
# Environment overrides (all optional):
#   DB2_IT_IMAGE          Db2 image (default below; must not be a floating tag)
#   DB2_IT_READY_TIMEOUT  seconds to wait for Db2 (default 480)
#   DB2_IT_REPORT_DIR     directory that receives the junit report (<suite>.junit.xml)
#                         or the soak log (soak.log)
#   DB2_IT_KEEP_CONTAINER 1 keeps the container after every run; "failure" keeps it
#                         only when a stage fails (diagnosis without leftovers)
#   DB2_IT_SOAK_SECONDS   measured soak duration after warmup for --soak (default 900)
#
# This script never reads .env files or DB2_* variables from the caller's
# environment: they are cleared before the tests run, so the tests can only
# reach the container started by this invocation.
set -Eeuo pipefail

SLUG="justybase-db2-node"
# Pinned Db2 LUW 12.1.5 Community image from the IBM Container Registry. The
# default image is verified against REQUIRED_DIGEST; an override must pin its own
# digest (image@sha256:... or DB2_IT_EXPECTED_DIGEST) or the run fails closed.
DEFAULT_IMAGE="icr.io/db2_community/db2:12.1.5.0"
REQUIRED_DIGEST="sha256:2de8151713c261843868c5c3411b57be6ae79d99d70a5b3022337836776bfda6"
IMAGE="${DB2_IT_IMAGE:-$DEFAULT_IMAGE}"
READY_TIMEOUT="${DB2_IT_READY_TIMEOUT:-480}"
# Options of this script are read before the DB2_* variables are cleared below.
REPORT_DIR_INPUT="${DB2_IT_REPORT_DIR:-}"
KEEP_CONTAINER="${DB2_IT_KEEP_CONTAINER:-0}"
SOAK_SECONDS_INPUT="${DB2_IT_SOAK_SECONDS:-900}"
DB_NAME="testdb"
DB_USER="db2inst1"
# --full only: second database for CONNECT-level switches and the TLS listener port.
SECOND_DB="testdb2"
TLS_CONTAINER_PORT=50001
TLS_DSN=""

# Expected digest: the image's own @sha256, an explicit override, or the default
# digest when the default image is used. Empty means a tag-only override.
case "$IMAGE" in
  *@sha256:*) IMAGE_DIGEST="${IMAGE##*@}" ;;
  *) IMAGE_DIGEST="" ;;
esac
if [ -n "${DB2_IT_EXPECTED_DIGEST:-}" ]; then
  EXPECTED_DIGEST="$DB2_IT_EXPECTED_DIGEST"
elif [ -n "$IMAGE_DIGEST" ]; then
  EXPECTED_DIGEST="$IMAGE_DIGEST"
elif [ -z "${DB2_IT_IMAGE:-}" ]; then
  EXPECTED_DIGEST="$REQUIRED_DIGEST"
else
  EXPECTED_DIGEST=""
fi

SUITE="smoke"
for arg in "$@"; do
  case "$arg" in
    --full) SUITE="full" ;;
    --parity) SUITE="parity" ;;
    --soak) SUITE="soak" ;;
    -h|--help) sed -n '2,/^set -Eeuo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) printf '[db2-it] FAIL: unknown argument: %s (see --help)\n' "$arg" >&2; exit 2 ;;
  esac
done

log()  { printf '[db2-it] %s\n' "$*"; }
warn() { printf '[db2-it] WARN: %s\n' "$*" >&2; }
die()  { printf '[db2-it] FAIL: %s\n' "$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STAGE="startup"
CONTAINER=""
WORKDIR=""
PASSWORD=""

# ---------------------------------------------------------------- cleanup ---
# Runs on every exit path. The original exit status is preserved; a container
# that could not be removed turns an otherwise green run into a failure.
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  set +e
  local cleanup_failed=0

  if [ "$rc" -ne 0 ]; then
    if [ "$rc" -eq 130 ] || [ "$rc" -eq 143 ]; then
      log "Interrupted during stage: $STAGE"
    else
      printf '[db2-it] FAIL: stage "%s" failed (exit %s)\n' "$STAGE" "$rc" >&2
      if [ -n "$CONTAINER" ]; then
        { echo "[db2-it] ---- docker ps -a ----"
          docker ps -a
          echo "[db2-it] ---- docker logs $CONTAINER (last 200 lines) ----"
          docker logs --tail 200 "$CONTAINER" 2>&1 | redact
          # Only the lines that name a log-full cause: the whole db2diag.log can hold
          # payloads and is never printed.
          echo "[db2-it] ---- db2diag lines for ADM1823E / SQL-964 (last 40) ----"
          docker exec "$CONTAINER" su - "$DB_USER" -c 'f=$(find /home -maxdepth 6 -name db2diag.log 2>/dev/null | head -n 1); [ -n "$f" ] && grep -n -E "ADM1823E|SQL0964|SQLCODE[^0-9]*-964|57011" "$f" | tail -n 40' 2>&1 | redact
        } >&2
      fi
    fi
  fi

  if [ -n "$CONTAINER" ] && { [ "$KEEP_CONTAINER" = "1" ] || { [ "$KEEP_CONTAINER" = "failure" ] && [ "$rc" -ne 0 ]; }; }; then
    warn "keeping container $CONTAINER (DB2_IT_KEEP_CONTAINER=$KEEP_CONTAINER); remove it with: docker rm -f -v $CONTAINER"
  elif [ -n "$CONTAINER" ]; then
    log "Removing Db2 container $CONTAINER..."
    docker rm -f -v "$CONTAINER" >/dev/null 2>&1
    if docker inspect "$CONTAINER" >/dev/null 2>&1; then
      warn "container $CONTAINER still exists; remove it with: docker rm -f -v $CONTAINER"
      cleanup_failed=1
    fi
  fi
  if [ -n "$WORKDIR" ]; then rm -rf "$WORKDIR"; fi

  if [ "$cleanup_failed" -ne 0 ] && [ "$rc" -eq 0 ]; then rc=1; fi
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

redact() { if [ -n "$PASSWORD" ]; then sed "s/$PASSWORD/[REDACTED]/g"; else cat; fi; }

# -------------------------------------------------------------- pre-flight ---
STAGE="pre-flight"
log "Checking environment..."
[ "$(uname -s)" = "Linux" ] || die "Linux is required (found $(uname -s))."
case "$(uname -m)" in
  x86_64|amd64) ;;
  *) die "x86_64 is required: the IBM Db2 Community image is linux/amd64 only (found $(uname -m))." ;;
esac
command -v docker >/dev/null 2>&1 || die "Docker CLI not found. Install Docker Engine and make sure 'docker' is on PATH."
docker info >/dev/null 2>&1 || die "Docker daemon is not reachable. Start Docker and check that your user may run 'docker info'."
case "$IMAGE" in
  *:latest|*:) die "DB2_IT_IMAGE must be pinned to an exact version, not a floating tag: $IMAGE" ;;
  *:*) ;;
  *) die "DB2_IT_IMAGE must include an explicit tag: $IMAGE" ;;
esac
[ -n "$EXPECTED_DIGEST" ] || die "DB2_IT_IMAGE override must pin a digest (image@sha256:...) or set DB2_IT_EXPECTED_DIGEST."
command -v node >/dev/null 2>&1 || die "node not found. Install Node.js 22 or newer."
command -v npm >/dev/null 2>&1 || die "npm not found. Install Node.js 22 or newer."

# ----------------------------------------------------------------- startup ---
RUN_TAG="$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
CONTAINER="${SLUG}-db2-it-${RUN_TAG}"
SCHEMA="IT_$(printf '%s' "$RUN_TAG" | tr 'a-f' 'A-F')"
PASSWORD="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/${SLUG}-db2-it.XXXXXX")"

STAGE="image pull"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  log "Pulling $IMAGE (first run only; the image is several GB)..."
  docker pull "$IMAGE" >/dev/null || die "could not pull $IMAGE"
fi

STAGE="image digest verification"
actual_digests="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "$IMAGE" 2>/dev/null || true)"
case "$actual_digests" in
  *"$EXPECTED_DIGEST"*) log "Image digest verified: $EXPECTED_DIGEST" ;;
  *) die "Db2 image digest mismatch for $IMAGE: expected $EXPECTED_DIGEST, found: ${actual_digests:-<none>}" ;;
esac

start_db2_container() {
STAGE="container start"
log "Starting IBM Db2 ($IMAGE)..."
# The password goes through an env file (mode 600) so it never appears in a
# process list. The file lives in WORKDIR and is removed by cleanup.
umask 077
cat > "$WORKDIR/db2.env" <<EOF
LICENSE=accept
DB2INSTANCE=$DB_USER
DB2INST1_PASSWORD=$PASSWORD
DBNAME=$DB_NAME
BLU=false
ENABLE_ORACLE_COMPATIBILITY=false
UPDATEAVAIL=NO
TO_CREATE_SAMPLEDB=false
REPODB=false
IS_OSXFS=false
PERSISTENT_HOME=false
HADR_ENABLED=false
ETCD_ENDPOINT=
ETCD_USERNAME=
ETCD_PASSWORD=
EOF
# CONTAINER is already set, so cleanup also removes a container that docker
# created but failed to start.
TLS_PUBLISH=()
if [ "$SUITE" = "full" ]; then TLS_PUBLISH=(--publish "127.0.0.1::$TLS_CONTAINER_PORT"); fi
docker run --detach --name "$CONTAINER" --privileged \
    --label "db2-it.repo=$SLUG" --label "db2-it.run=$RUN_TAG" \
    --publish 127.0.0.1::50000 ${TLS_PUBLISH[@]+"${TLS_PUBLISH[@]}"} --env-file "$WORKDIR/db2.env" "$IMAGE" >/dev/null \
  || die "docker run failed"
log "Container: $CONTAINER"

PORT_LINE="$(docker port "$CONTAINER" 50000/tcp | head -n 1)"
HOST_PORT="${PORT_LINE##*:}"
case "$HOST_PORT" in ''|*[!0-9]*) die "could not determine the mapped host port (got '$PORT_LINE')" ;; esac
log "Host port: $HOST_PORT"

# --------------------------------------------------------------- readiness ---
# Ready = the image's setup finished, the database accepts a real SQL
# statement, and the mapped TCP port accepts connections.
db2_sql() {
  docker exec "$CONTAINER" su - "$DB_USER" -c \
    "db2 connect to $DB_NAME >/dev/null && db2 -x \"$1\" ; rc=\$?; db2 terminate >/dev/null 2>&1; exit \$rc"
}
tcp_open() { (exec 3<>"/dev/tcp/127.0.0.1/$HOST_PORT") 2>/dev/null; }

STAGE="waiting for Db2"
log "Waiting for Db2 (timeout ${READY_TIMEOUT}s)..."
started=$SECONDS
next_progress=30
ready=0
while [ $((SECONDS - started)) -lt "$READY_TIMEOUT" ]; do
  state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null || echo gone)"
  if [ "$state" != "running" ]; then warn "Db2 container stopped unexpectedly (state: $state)."; return 1; fi
  # Command substitution reads the whole log first; a pipe into 'grep -q' can fail
  # under pipefail when grep exits early and docker logs gets SIGPIPE.
  if [[ "$(docker logs "$CONTAINER" 2>&1)" == *'Setup has completed'* ]]; then
    probe="$(db2_sql "VALUES 1" 2>&1)" && probe_rc=0 || probe_rc=$?
    if [ "$probe_rc" -eq 0 ] && tcp_open; then ready=1; break; fi
    # Setup reports completion but the database does not exist (observed when the
    # instance setup fails silently, e.g. SQL0204N during database creation). Waiting
    # longer cannot help, so the attempt is abandoned at once.
    if [[ "$probe" == *SQL1013N* ]]; then
      warn "setup completed but database $DB_NAME was not created (SQL1013N); abandoning this container"
      return 1
    fi
  fi
  elapsed=$((SECONDS - started))
  if [ "$elapsed" -ge "$next_progress" ]; then log "  still waiting... ${elapsed}s"; next_progress=$((next_progress + 30)); fi
  sleep 3
done
if [ "$ready" -ne 1 ]; then warn "Db2 did not become ready within ${READY_TIMEOUT} seconds."; return 1; fi
log "Db2 is ready ($((SECONDS - started))s)"
return 0
}

# A fresh container is started again (up to three attempts) when one never becomes ready:
# the image setup sometimes stalls or leaves the instance without its database. Test
# failures are never retried.
attempt=1
until start_db2_container; do
  [ "$attempt" -lt 3 ] || die "Db2 did not become ready in three attempts"
  attempt=$((attempt + 1))
  warn "removing $CONTAINER and starting a fresh container (attempt $attempt of 3)"
  docker rm -f -v "$CONTAINER" >/dev/null 2>&1 || true
  RUN_TAG="$(od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
  CONTAINER="${SLUG}-db2-it-${RUN_TAG}"
done

# ------------------------------------------------------ full-suite extras ---
# The full suite also covers TLS and a database switch. Both are provisioned only
# here; the smoke and parity lanes keep their small footprint.
port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

provision_tls() {
  STAGE="tls listener"
  log "Provisioning the TLS listener (ephemeral CA, certificate identity 127.0.0.1)..."
  local dir="$WORKDIR/tls" kdb_pw p12_pw tls_port
  mkdir -p "$dir"
  chmod 700 "$dir"
  kdb_pw="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
  p12_pw="$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')"
  openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 -subj "/CN=justybase-db2-node ephemeral CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$dir/ca.key" -out "$dir/ca.pem" >/dev/null 2>&1 || die "could not create the ephemeral CA"
  openssl req -newkey rsa:2048 -nodes -sha256 -subj "/CN=127.0.0.1" \
    -keyout "$dir/server.key" -out "$dir/server.csr" >/dev/null 2>&1 || die "could not create the server key"
  printf '%s\n' "subjectAltName=IP:127.0.0.1" "basicConstraints=critical,CA:FALSE" \
    "keyUsage=critical,digitalSignature,keyEncipherment" "extendedKeyUsage=serverAuth" > "$dir/server.ext"
  openssl x509 -req -in "$dir/server.csr" -CA "$dir/ca.pem" -CAkey "$dir/ca.key" -CAcreateserial -sha256 -days 2 \
    -extfile "$dir/server.ext" -out "$dir/server.pem" >/dev/null 2>&1 || die "could not sign the server certificate"
  openssl pkcs12 -export -inkey "$dir/server.key" -in "$dir/server.pem" -certfile "$dir/ca.pem" -name db2server \
    -passout "pass:$p12_pw" -out "$dir/server.p12" >/dev/null 2>&1 || die "could not export the PKCS#12 bundle"
  chmod 600 "$dir/ca.key" "$dir/server.key" "$dir/server.p12"

  # GSKit's certificate tool needs the system ICU runtime, which the image omits.
  docker exec -u root "$CONTAINER" bash -c 'rpm -q libicu >/dev/null 2>&1 || dnf install -y libicu >/dev/null 2>&1' \
    || die "could not install the ICU runtime for GSKit"
  docker cp "$dir/server.p12" "$CONTAINER:/tmp/server.p12" >/dev/null || die "could not copy the server bundle"
  docker cp "$dir/ca.pem" "$CONTAINER:/tmp/ca.pem" >/dev/null || die "could not copy the CA"
  docker exec -u root -e "KDBPW=$kdb_pw" -e "P12PW=$p12_pw" "$CONTAINER" bash -c '
    set -e
    HOME_DIR=$(ls -d /opt/ibm/db2/V* | head -1)
    export LD_LIBRARY_PATH="$HOME_DIR/lib64/gskit_db2:$HOME_DIR/lib64"
    G="$HOME_DIR/gskit/bin/gsk9certutil_64"
    D=/database/config/'"$DB_USER"'/tls
    mkdir -p "$D"; cd "$D"; rm -f server.kdb server.sth server.rdb server.crl
    "$G" -keydb -create -db server.kdb -pw "$KDBPW" -type cms -stash
    "$G" -cert -add -db server.kdb -pw "$KDBPW" -label ephemeral-ca -file /tmp/ca.pem -trust enable
    "$G" -cert -import -db /tmp/server.p12 -pw "$P12PW" -type pkcs12 -target server.kdb -target_pw "$KDBPW" -target_type cms -label db2server -new_label db2server
    rm -f /tmp/server.p12 /tmp/ca.pem
    chown -R '"$DB_USER"': "$D"; chmod 600 "$D"/*' >/dev/null 2>&1 || die "could not create the server keystore"

  # TCP stays on for the plain port; the SSL listener is added next to it.
  docker exec "$CONTAINER" su - "$DB_USER" -c "
    db2 update dbm cfg using SSL_SVR_KEYDB /database/config/$DB_USER/tls/server.kdb SSL_SVR_STASH /database/config/$DB_USER/tls/server.sth SSL_SVR_LABEL db2server SSL_SVCENAME $TLS_CONTAINER_PORT SSL_VERSIONS TLSV12 >/dev/null &&
    db2set DB2COMM=TCPIP,SSL &&
    db2stop force >/dev/null 2>&1; db2start > /tmp/db2start.out 2>&1; cat /tmp/db2start.out; ! grep -q SQL5043N /tmp/db2start.out" >/dev/null \
    || die "the SSL listener did not start"

  tls_port="$(docker port "$CONTAINER" "$TLS_CONTAINER_PORT/tcp" | head -n 1)"
  tls_port="${tls_port##*:}"
  case "$tls_port" in ''|*[!0-9]*) die "could not determine the mapped TLS port" ;; esac
  local waited=0
  until db2_sql "VALUES 1" >/dev/null 2>&1 && port_open "$HOST_PORT" && port_open "$tls_port"; do
    waited=$((waited + 1))
    [ "$waited" -le 120 ] || die "Db2 did not accept connections after the TLS listener restart"
    sleep 2
  done
  TLS_DSN="db2s://${DB_USER}:${PASSWORD}@127.0.0.1:${tls_port}/${DB_NAME}?ssl_root_ca=${dir}/ca.pem"
  log "TLS listener ready on 127.0.0.1:$tls_port"
}

run_soak() {
  STAGE="resource soak"
  local tag duration log_file
  tag="$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n' | tr 'a-f' 'A-F')"
  duration="$SOAK_SECONDS_INPUT"
  case "$duration" in ''|*[!0-9]*) die "DB2_IT_SOAK_SECONDS must be a whole number of seconds" ;; esac
  log "Seeding soak fixtures (tag $tag)..."
  node benchmarks/resource-soak-seed.cjs "$tag" || die "could not seed the soak fixtures"
  log "Running the resource soak: 10 warmup cycles, then ${duration}s measured (RSS budget 512 MiB)..."
  log_file="$WORKDIR/soak.log"
  DB2_SOAK_DURATION_SECONDS="$duration" node benchmarks/resource-soak.cjs \
    "SO_${tag}_N" "SO_${tag}_ND" "SO_${tag}_X" "SO_${tag}_XD" "SO_${tag}_P" 2>&1 | tee "$log_file" \
    || die "resource soak failed"
  grep -q '"type":"resource-fd-summary"' "$log_file" || die "resource soak did not reach its final FD summary"
  if [ -n "$REPORT_DIR_INPUT" ]; then
    mkdir -p "$REPORT_DIR_INPUT"
    cp "$log_file" "$REPORT_DIR_INPUT/soak.log"
    log "soak log: $REPORT_DIR_INPUT/soak.log"
  fi
}

create_second_database() {
  STAGE="second database"
  log "Creating second database $SECOND_DB..."
  docker exec "$CONTAINER" su - "$DB_USER" -c "db2 create database $SECOND_DB >/dev/null" >/dev/null \
    || die "could not create database $SECOND_DB"
}

if [ "$SUITE" = "full" ]; then
  provision_tls
  create_second_database
fi

# ---------------------------------------------------------------- log capacity ---
# Same provisioning rule as the C# qualification (db2-conformance manifest, LOGSECOND
# 512): an atomic import keeps its whole transaction in the active log, so the
# default 22 secondary files (about 152 MiB) can run out during a long soak and fail
# with SQL-964/57011. The change is an online test-database setting, raised only, and
# recorded before and after. It is not a driver change: -964 is still reported.
LOG_SECONDARY_TARGET=512
log_config() {
  docker exec "$CONTAINER" su - "$DB_USER" -c "db2 get db cfg for $DB_NAME" 2>&1 \
    | grep -E '\((LOGFILSIZ|LOGPRIMARY|LOGSECOND)\)' || true
}

configure_log_capacity() {
  STAGE="log capacity"
  local before after current
  before="$(log_config)"
  [ -n "$before" ] || die "could not read the log configuration of $DB_NAME"
  current="$(printf '%s\n' "$before" | sed -n 's/.*(LOGSECOND) = \([0-9]*\).*/\1/p')"
  if [ -n "$current" ] && [ "$current" -lt "$LOG_SECONDARY_TARGET" ]; then
    log "Raising LOGSECOND $current -> $LOG_SECONDARY_TARGET on $DB_NAME (online, no restart)..."
    docker exec "$CONTAINER" su - "$DB_USER" -c "db2 update db cfg for $DB_NAME using LOGSECOND $LOG_SECONDARY_TARGET >/dev/null" \
      || die "could not raise LOGSECOND on $DB_NAME"
  fi
  after="$(log_config)"
  log "log configuration of $DB_NAME before: $(printf '%s' "$before" | tr -s ' ' | tr '\n' ' ')"
  log "log configuration of $DB_NAME after:  $(printf '%s' "$after" | tr -s ' ' | tr '\n' ' ')"
  if [ -n "$REPORT_DIR_INPUT" ]; then
    mkdir -p "$REPORT_DIR_INPUT"
    printf 'before:\n%s\nafter:\n%s\n' "$before" "$after" > "$REPORT_DIR_INPUT/db-log-config.txt"
  fi
}
configure_log_capacity

# ---------------------------------------------------------------- fixtures ---
STAGE="preparing fixtures"
log "Preparing fixtures (schema $SCHEMA)..."
db2_sql "CREATE SCHEMA $SCHEMA" >/dev/null || die "could not create schema $SCHEMA"
db2_sql "SELECT COUNT(*) FROM SYSCAT.SCHEMATA WHERE SCHEMANAME = '$SCHEMA'" | grep -qx '[[:space:]]*1' \
  || die "schema $SCHEMA was not created"

# ------------------------------------------------------------- test run ------
# Clear everything that could point the tests at some other database, then set
# exactly one connection description: the container started above.
while IFS= read -r name; do unset "$name"; done < <(compgen -e | grep '^DB2_' || true)
export DB2_TEST_DSN="db2://${DB_USER}:${PASSWORD}@127.0.0.1:${HOST_PORT}/${DB_NAME}"
export DB2_LIVE_TEST_HOST="127.0.0.1"
export DB2_LIVE_TEST_PORT="$HOST_PORT"
export DB2_LIVE_TEST_DATABASE="$DB_NAME"
export DB2_LIVE_TEST_USER="$DB_USER"
export DB2_LIVE_TEST_PASSWORD="$PASSWORD"
export DB2_IT_SCHEMA="$SCHEMA"
if [ "$SUITE" = "full" ]; then
  export DB2_IT_SECOND_DATABASE="$SECOND_DB"
  export DB2_TLS_TEST_DSN="$TLS_DSN"
fi
# Stops tests/load-env.cjs from reading any .env file: with this flag set and
# DB2_TEST_DSN present it returns immediately.
export DB2_ENV_LOADED=1

STAGE="running tests"
log "Running integration tests ($SUITE suite)..."
cd "$REPO_ROOT"
[ -d node_modules ] || { log "Installing locked dependencies (npm ci --ignore-scripts)..."; npm ci --ignore-scripts; }
npm run build
if [ "$SUITE" = "soak" ]; then
  run_soak
  log "PASS"
  exit 0
fi
# Pinned test-only oracle for differential parity. Installed with --no-save so it
# never enters package.json, package-lock.json, SBOM or the published tarball;
# the driver itself never requires IBM binaries at runtime.
IBMDB_PIN="ibm_db@4.0.1"
case "$SUITE" in
  smoke) LIVE_FILES=(tests/live/smoke.test.js) ;;
  parity) LIVE_FILES=(tests/live/smoke.test.js tests/live/parity-ibmdb.test.js) ;;
  full) LIVE_FILES=(tests/live/*.test.js) ;;
esac
if [ "$SUITE" != "smoke" ]; then
  log "Installing test-only oracle $IBMDB_PIN (--no-save, not packaged)..."
  npm install --no-save "$IBMDB_PIN"
  export DB2_PARITY_IBMDB=1
  export DB2_PARITY_REPORT="$REPO_ROOT/parity-report.json"
fi

# Every run writes a junit report that is checked for skipped tests. A skip is only
# accepted when the reason matches an allow pattern below; anything else fails the run.
JUNIT="$WORKDIR/$SUITE.junit.xml"
STAGE="running tests"
node --test --test-concurrency=1 \
  --test-reporter=spec --test-reporter-destination=stdout \
  --test-reporter=junit --test-reporter-destination="$JUNIT" \
  "${LIVE_FILES[@]}"

ALLOW=()
if [ "$SUITE" = "full" ]; then
  # The IBM import cross-check needs the IBM .NET oracle (DB2_IBM_IMPORT_ORACLE), which
  # is not provisioned by this repository. It is the only accepted skip of this suite.
  ALLOW=(--allow-skip 'DB2_IBM_IMPORT_ORACLE is not set')
fi
STAGE="skip gate"
node tools/ci/check-junit-no-skips.cjs "$JUNIT" ${ALLOW[@]+"${ALLOW[@]}"}

if [ -n "$REPORT_DIR_INPUT" ]; then
  mkdir -p "$REPORT_DIR_INPUT"
  cp "$JUNIT" "$REPORT_DIR_INPUT/$SUITE.junit.xml"
  log "junit report: $REPORT_DIR_INPUT/$SUITE.junit.xml"
fi

log "PASS"
