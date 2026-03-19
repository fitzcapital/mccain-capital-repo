#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
ROOT_DIR="$(cd "$(dirname "${SCRIPT_PATH}")/.." && pwd)"
DATA_DIR="${1:-${DATA_DIR:-$ROOT_DIR/persistent-data}}"
DB_PATH="${DB_PATH:-$DATA_DIR/journal.db}"
LOG_FILE="${LOG_FILE:-}"
SQLITE_BIN="${SQLITE_BIN:-$(command -v sqlite3 || echo /usr/bin/sqlite3)}"
XATTR_BIN="${XATTR_BIN:-$(command -v xattr || echo /usr/bin/xattr)}"

log() {
  local msg="$1"
  if [[ -n "$LOG_FILE" ]]; then
    mkdir -p "$(dirname "$LOG_FILE")"
    echo "$msg" >> "$LOG_FILE"
  else
    echo "$msg"
  fi
}

has_container_xattr() {
  local path="$1"
  [[ -e "$path" ]] || return 1
  "$XATTR_BIN" -p user.containers.override_stat "$path" >/dev/null 2>&1 || \
    "$XATTR_BIN" -p security.selinux "$path" >/dev/null 2>&1
}

if [[ ! -f "$DB_PATH" ]]; then
  exit 0
fi

if [[ ! -x "$SQLITE_BIN" ]] || [[ ! -x "$XATTR_BIN" ]]; then
  exit 0
fi

db_wal="${DB_PATH}-wal"
db_shm="${DB_PATH}-shm"
needs_repair=0

if has_container_xattr "$DB_PATH"; then
  needs_repair=1
fi

if [[ "$needs_repair" -ne 1 ]]; then
  exit 0
fi

timestamp="$(date '+%Y%m%d_%H%M%S')"
tmp_path="${DB_PATH}.repair.${timestamp}.tmp"
backup_path="${DB_PATH}.badxattr.${timestamp}"

log "[sqlite-repair] detected container xattrs on $DB_PATH; rebuilding SQLite file"
"$SQLITE_BIN" "$DB_PATH" ".timeout 10000" ".backup $tmp_path"
mv "$DB_PATH" "$backup_path"
mv "$tmp_path" "$DB_PATH"
chmod 0644 "$DB_PATH" >/dev/null 2>&1 || true
rm -f "$db_wal" "$db_shm"
log "[sqlite-repair] replaced $DB_PATH and saved original to $backup_path"
