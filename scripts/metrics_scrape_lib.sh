# shellcheck shell=bash
# Shared plumbing for the benchmark harnesses that read a node's own flush latency off its `/metrics`
# (scripts/loadtest-ceiling.sh and benchmark/docker-cluster.sh): log in to the dashboard, scrape the
# Prometheus exposition into a file, and say why when either could not be done. The window itself is
# computed by `mix malachi.loadtest.ceiling flush-window` from two scrapes (Malachi.Loadtest.FlushWindow).
#
# Transport is the caller's, because the two harnesses reach the dashboard differently (curl on the host,
# `docker exec ... wget` inside a node). Before calling anything here, define:
#
#   ms_http_post PATH JSON    POST JSON to PATH; print the response body and return 0 on a 2xx.
#   ms_http_get PATH TOKEN    GET PATH with `Authorization: Bearer TOKEN` and `Accept: text/plain`;
#                             print the body and return 0 on a 2xx.
#
# On anything else both print one line and return non-zero: `HTTP <status>` when the server answered, or
# the transport's own reason when it did not.
#
# Budget: the dashboard rate-limits authenticated requests per client IP (10 a minute by default) and
# binds a session to the IP that logged in. A login plus one scrape before and one after a window is
# three requests from one address, which fits; a harness must not scrape more often than that per node.
#
# Nothing here exits: every function prints a reason and returns non-zero, so a failed scrape is recorded
# next to a throughput that is still valid instead of aborting the run.

# Turns a transport failure into the words a result records.
ms_explain() { # ms_explain <transport output>
  case "$1" in
    # Sessions live in the node's memory, so a token the node no longer knows usually means it restarted.
    "HTTP 401"*) echo "$1 (the session is gone; the node may have restarted)" ;;
    "HTTP 403"*) echo "$1 (the dashboard refused the credentials)" ;;
    "HTTP 429"*) echo "$1 (the dashboard rate limit tripped)" ;;
    "") echo "no answer" ;;
    *) echo "$1" ;;
  esac
}

# Logs in as USER and prints the session token.
ms_login() { # ms_login <user> <pass>
  local payload body token
  payload="$(jq -cn --arg u "$1" --arg p "$2" '{username: $u, password: $p}')" || {
    echo "login failed: jq could not build the request"
    return 1
  }
  if ! body="$(ms_http_post /login "$payload")"; then
    echo "login failed: $(ms_explain "$body")"
    return 1
  fi
  token="$(jq -r '.token // empty' 2> /dev/null <<< "$body")"
  if [ -z "$token" ]; then
    echo "login failed: the answer carried no token"
    return 1
  fi
  printf '%s\n' "$token"
}

# Writes one Prometheus exposition to FILE.
ms_scrape() { # ms_scrape <token> <file>
  local body
  if ! body="$(ms_http_get /metrics "$1")"; then
    echo "scrape failed: $(ms_explain "$body")"
    return 1
  fi
  printf '%s\n' "$body" > "$2" || {
    echo "scrape failed: cannot write $2"
    return 1
  }
}

# One flush window's files share a prefix: <prefix>.before.prom and <prefix>.after.prom hold the two
# scrapes, and <prefix>.error.txt says why the window could not be opened.

# Opens the window: the scrape when the measured window starts. A no-op without a token, whose login
# failure the caller already holds.
ms_open_window() { # ms_open_window <token or empty> <prefix>
  local reason
  [ -n "$1" ] || return 0
  if ! reason="$(ms_scrape "$1" "$2.before.prom")"; then
    echo "$reason" > "$2.error.txt"
  fi
}

# Closes the window: the scrape when the measured window ends, taken while the node is still up since its
# counters die with it. Prints nothing when both scrapes are on disk, or the one reason there is no window.
ms_close_window() { # ms_close_window <token or empty> <login failure> <prefix>
  if [ -z "$1" ]; then
    echo "$2"
  elif [ -s "$3.error.txt" ]; then
    cat "$3.error.txt"
  elif [ ! -s "$3.before.prom" ]; then
    echo "no measured window was signaled, so nothing opened the flush window"
  else
    ms_scrape "$1" "$3.after.prom"
  fi
}
