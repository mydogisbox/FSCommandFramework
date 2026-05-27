#!/bin/bash

set -eo pipefail

cd "$(dirname "$0")/.."

DB_CONTAINER="fscommandframework-db"
DB_PORT=5432
DB_NAME="commandframework_test"
DB_USER="postgres"
DB_PASS="postgres"

TEST_PROJECT="tests/FSCommandFramework.Tests"
TEST_DLL="tests/FSCommandFramework.Tests/bin/Debug/net10.0/FSCommandFramework.Tests.dll"
MIGRATIONS_DIR="src/FSCommandFramework.Postgres/Migrations"

needs_rebuild() {
  [ ! -f "$TEST_DLL" ] && return 0
  [ -n "$(find src tests \( -name "*.fs" -o -name "*.fsproj" \) -newer "$TEST_DLL" -print -quit)" ]
}

# ── Database ──────────────────────────────────────────────────────────────────

ensure_db() {
  if docker ps --filter "name=^${DB_CONTAINER}$" --format '{{.Names}}' | grep -q "^${DB_CONTAINER}$"; then
    return
  fi

  if docker ps -a --filter "name=^${DB_CONTAINER}$" --format '{{.Names}}' | grep -q "^${DB_CONTAINER}$"; then
    echo "→ Starting existing database container..."
    docker start "$DB_CONTAINER"
  else
    echo "→ Creating database container..."
    docker run -d \
      --name "$DB_CONTAINER" \
      -e POSTGRES_DB="$DB_NAME" \
      -e POSTGRES_USER="$DB_USER" \
      -e POSTGRES_PASSWORD="$DB_PASS" \
      -p "${DB_PORT}:5432" \
      postgres:16
  fi

  echo -n "  Waiting for Postgres"
  for i in {1..30}; do
    if docker exec "$DB_CONTAINER" pg_isready -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; then
      echo " ready"
      return
    fi
    echo -n "."
    sleep 1
    if [ $i -eq 30 ]; then echo " timed out"; exit 1; fi
  done
}

run_migrations() {
  docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -q -c "
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );"

  local applied
  applied=$(docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -t -c \
    "SELECT version FROM schema_migrations;" | tr -d '[:space:]')

  for sql_file in "$MIGRATIONS_DIR"/*.sql; do
    version=$(basename "$sql_file")
    if echo "$applied" | grep -qF "$version"; then
      echo "  ✓ $version"
    else
      echo "  → Applying $version..."
      docker exec -i "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -q < "$sql_file"
      docker exec "$DB_CONTAINER" psql -U "$DB_USER" -d "$DB_NAME" -q -c \
        "INSERT INTO schema_migrations (version) VALUES ('$version');"
    fi
  done
}

# ── Build ─────────────────────────────────────────────────────────────────────

if needs_rebuild; then
  echo "→ Building..."
  dotnet build -nologo -v q
else
  echo "→ No source changes — skipping build."
fi

# ── Database ──────────────────────────────────────────────────────────────────

ensure_db
echo "→ Running migrations..."
run_migrations

# ── Tests ─────────────────────────────────────────────────────────────────────

echo "→ Running tests..."
set +e
dotnet test "$TEST_PROJECT" --no-build -nologo
TEST_EXIT=$?
set -e

exit $TEST_EXIT
