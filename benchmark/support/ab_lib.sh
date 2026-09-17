# The driver half of every A/B that interleaves by RUN (see support/ab_run.exs for the analysis half).
# Sourced, not run. The sourcing script sets BASELINE, BRANCH and OUT, and defines
# `run_one CASE REP ARM`, which writes one sample to "$OUT/CASE/REP-ARM.out".
#
# Born inside store_error_path_ab.sh (issue #147), moved here for the rate limiter A/B (issue #151).

say() { printf '[ab] %s\n' "$*"; }

build() {
  say "building $1"
  (
    cd "$1"
    for attempt in 1 2 3; do
      mix deps.get >/dev/null && break
      say "deps.get failed (attempt $attempt/3)"
      [ "$attempt" -lt 3 ] && sleep 5
    done
    mix compile >/dev/null
  )
}

tree_of() {
  if [ "$1" = branch ]; then echo "$BRANCH"; else echo "$BASELINE"; fi
}

# Shuffled per repetition: a rotation keeps every arm behind the same neighbour, which PreallocAB caught
# biasing an identical arm by 850us.
shuffled_arms() { printf 'main_a1\nmain_a2\nbranch\n' | shuf; }

run_case() {
  kase=$1 reps=$2
  [ "$reps" -gt 0 ] || return 0
  mkdir -p "$OUT/$kase"
  # One discarded pass per arm: the first run pays for cold caches and first-touch allocation, which is a
  # property of the harness rather than of either tree.
  for arm in $(shuffled_arms); do run_one "$kase" warm "$arm"; done
  rep=1
  while [ "$rep" -le "$reps" ]; do
    order=$(shuffled_arms | tr '\n' ' ')
    say "$kase rep $rep/$reps: $order"
    for arm in $order; do run_one "$kase" "$rep" "$arm"; done
    rep=$((rep + 1))
  done
}
