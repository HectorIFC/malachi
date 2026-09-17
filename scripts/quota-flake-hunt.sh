#!/usr/bin/env bash
# Runs the whole test suite REPS times, the way CI runs it, and counts how often a publish or subscribe
# quota admits over its limit (issue #151). The suite is the unit of repetition on purpose: both CI
# occurrences came from full runs, and a loop over the enforcement file alone does not recreate what runs
# alongside it.
#
# MALACHI_QUOTA_FORENSICS=strict makes test/support/quota_forensics.ex report every over-limit admission,
# with the limiter pid, time warp mode, clock offset, window, caps and counters, instead of rerunning a
# test that crossed a window boundary, and print a line for every straddle that changed no assertion.
# After each iteration the OS clock discipline is recorded, so a report can be lined up with a clock step.
#
# Only a Linux run counts. Usage: scripts/quota-flake-hunt.sh REPS OUT_DIR
# Exits 1 when any iteration showed the quota symptom; other failures are recorded, not fatal, because
# the suite has known unrelated flakes and they must not read as this one.
set -uo pipefail

reps=${1:?usage: quota-flake-hunt.sh REPS OUT_DIR}
out=${2:?usage: quota-flake-hunt.sh REPS OUT_DIR}
case "$reps" in
  '' | *[!0-9]* | 0*) echo "REPS must be a positive integer, got '$reps'"; exit 2 ;;
esac

export MALACHI_QUOTA_FORENSICS=strict
mkdir -p "$out"

# Everything that decides how the VM's clock behaves, once: the time warp mode is the difference between a
# clock step being slewed (no_time_warp) and being applied at once (multi_time_warp).
{
  echo "nproc: $(nproc)"
  uname -a
  elixir -e 'IO.inspect(%{time_warp_mode: :erlang.system_info(:time_warp_mode), time_correction: :erlang.system_info(:time_correction), otp: :erlang.system_info(:otp_release), schedulers_online: :erlang.system_info(:schedulers_online)})'
} | tee "$out/environment.txt"

clock_state() {
  date -u +%FT%T.%NZ
  chronyc tracking 2>/dev/null || timedatectl timesync-status 2>/dev/null || timedatectl 2>/dev/null || echo "no clock discipline tool"
}

quota=0
other=0
straddles=0
for i in $(seq 1 "$reps"); do
  log="$out/iteration-$i.log"
  { echo "== before iteration $i"; clock_state; } >>"$out/clock.log"
  started=$(date +%s)

  if mix coveralls.json >"$log" 2>&1; then status=pass; else status=fail; fi

  { echo "== after iteration $i"; clock_state; } >>"$out/clock.log"
  seen=$(grep -c 'quota-forensics straddle' "$log" || true)
  straddles=$((straddles + seen))

  if grep -qE 'expected \{:error, "rate_limited"\}|crossed a window boundary' "$log"; then
    quota=$((quota + 1))
    status="QUOTA"
    echo "::error::iteration $i admitted over a quota limit, see iteration-$i.log"
  elif [ "$status" = fail ]; then
    other=$((other + 1))
    echo "::warning::iteration $i failed for another reason: $(grep -m1 -E '^\s+[0-9]+\) test' "$log" | sed 's/^ *//')"
  else
    rm -f "$log"
  fi

  echo "iteration $i/$reps: $status in $(($(date +%s) - started))s, $seen straddles"
done

summary="quota failures/iterations: $quota/$reps, other failures: $other, straddles seen: $straddles"
echo "$summary" | tee "$out/summary.txt"
[ "$quota" -eq 0 ]
