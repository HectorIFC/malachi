#!/bin/sh
# Keeps a Malachi dashboard token on disk per node, for Prometheus to scrape with.
#
# Why this exists: /metrics is authenticated (any user), and a Malachi session expires
# (MALACHI_SESSION_TIMEOUT_SEC, one hour by default). Prometheus cannot log in, so the alternatives
# were a token pasted into the scrape config that silently stops working after an hour, or turning the
# endpoint's authentication off. This is the third option, and it is about thirty lines.
#
# ONE TOKEN PER NODE, which is not an optimization but a requirement: users, ACLs and lockouts are
# replicated across the cluster through the auth `ra` group, and sessions are NOT. They live in a local
# ETS table (Malachi.Auth, @sessions_table), so a session minted on node 1 does not exist on node 2,
# which answers `invalid_session` and a 403. A cluster therefore needs one login per node, and
# Prometheus needs one scrape job per node to point at the right file.
#
# Prometheus re-reads `credentials_file` on every scrape, so refreshing the files is enough; nothing
# needs reloading.
#
# Environment:
#   MALACHI_TARGETS   comma-separated name=url pairs, e.g. "malachi1=http://malachi1:4041,malachi2=..."
#   TOKEN_DIR         where <name>.token is written (default /token)
#   MALACHI_METRICS_USER, MALACHI_METRICS_PASS, REFRESH_SECONDS
set -u

MALACHI_TARGETS="${MALACHI_TARGETS:-malachi=http://malachi:4041}"
TOKEN_DIR="${TOKEN_DIR:-/token}"
MALACHI_METRICS_USER="${MALACHI_METRICS_USER:-admin}"
MALACHI_METRICS_PASS="${MALACHI_METRICS_PASS:-admin123}"
# Half the default session lifetime, so a token is replaced with roughly thirty minutes still on it.
# Refreshing at the boundary would leave a window where every scrape 401s while the next attempt is
# still sleeping.
REFRESH_SECONDS="${REFRESH_SECONDS:-1800}"

log() { echo "[metrics-token] $1"; }

# Written to a neighbouring file and moved into place: Prometheus reads these on a timer of its own,
# and a partially written token is a 401 that looks like a credentials problem rather than a race.
write_token() {
  printf '%s' "$2" > "${TOKEN_DIR}/${1}.token.new" && mv "${TOKEN_DIR}/${1}.token.new" "${TOKEN_DIR}/${1}.token"
}

fetch_token() {
  curl -sf -X POST "${1}/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"${MALACHI_METRICS_USER}\",\"password\":\"${MALACHI_METRICS_PASS}\"}" \
    2>/dev/null | sed -n 's/.*"token":"\([^"]*\)".*/\1/p'
}

mkdir -p "$TOKEN_DIR"

while true; do
  missing=0

  # `set -- ` with IFS split rather than a bash array: this runs in the curl image's ash.
  IFS=','
  for target in $MALACHI_TARGETS; do
    name="${target%%=*}"
    url="${target#*=}"
    token="$(fetch_token "$url")"

    if [ -n "$token" ]; then
      write_token "$name" "$token"
    else
      missing=$((missing + 1))
      log "login to $name ($url) failed"
    fi
  done
  unset IFS

  if [ "$missing" -eq 0 ]; then
    log "all tokens refreshed, next in ${REFRESH_SECONDS}s"
    sleep "$REFRESH_SECONDS"
  else
    # Usually a node still booting. Retrying quickly rather than sleeping out the full refresh
    # interval is what keeps a slow starter from being unscraped for half an hour.
    log "$missing target(s) not ready, retrying in 5s"
    sleep 5
  fi
done
