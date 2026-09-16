# Summarizes the OUT lines of benchmark/docker-cluster.sh (read whole, with `jq -rs -f`) into the markdown
# the dispatch job publishes. A case counts as measured only when its outcome starts with "ok": a case
# that failed a check (wrong filesystem, preallocation missing) measured the wrong path, and one without a
# result measured nothing. Those are listed as failures instead.
#
# The protocol is the one benchmark/README.md states: repetitions of each mode, interleaved, and a
# difference between the modes is claimed only when their min to max ranges do not overlap, and never
# from a single run of either mode, whose range is a point with no spread to measure noise by.

def median:
  sort | length as $n
  | if $n == 0 then null
    elif $n % 2 == 1 then .[($n - 1) / 2]
    else (.[$n / 2 - 1] + .[$n / 2]) / 2
    end;

def spread(f): [.[] | f | select(. != null)] | {runs: length, median: median, min: min, max: max};

def show: if .median == null then "n/a" else "\(.median) (\(.min) to \(.max))" end;

def verdict($name; $tmpfs; $disk):
  if $tmpfs.median == null or $disk.median == null then "\($name): not comparable, one mode has no result"
  elif $tmpfs.runs < 2 or $disk.runs < 2 then
    "\($name): a single run of a mode has no noise floor, no difference is claimed (tmpfs \($tmpfs | show), disk \($disk | show))"
  elif $disk.max < $tmpfs.min or $tmpfs.max < $disk.min then
    "\($name): ranges are disjoint (tmpfs \($tmpfs | show), disk \($disk | show))"
  else "\($name): ranges overlap, no difference is claimed (tmpfs \($tmpfs | show), disk \($disk | show))"
  end;

map(select(.outcome | startswith("ok"))) as $measured
| map(select(.outcome | startswith("ok") | not)) as $failed
| [
    "| RF | data | runs | rec/s, median (min to max) | p50 ms | p99 ms | filesystem |",
    "|---|---|---|---|---|---|---|"
  ]
  + [
      $measured | group_by([.rf, .data_mode])[]
      | "| \(.[0].rf) | \(.[0].data_mode) | \(length) | \(spread(.loadtest.records_per_s) | show) "
        + "| \(spread(.loadtest.latency_ms.p50) | show) | \(spread(.loadtest.latency_ms.p99) | show) "
        + "| \([.[].nodes[].fstype] | unique | join(", ")) |"
    ]
  + ["", "Disk against tmpfs, per RF (a difference counts only when the min to max ranges are disjoint):"]
  + [
      $measured | group_by(.rf)[]
      | map(select(.data_mode == "tmpfs")) as $tmpfs
      | map(select(.data_mode == "disk")) as $disk
      | "- RF \(.[0].rf): "
        + verdict("rec/s"; $tmpfs | spread(.loadtest.records_per_s); $disk | spread(.loadtest.records_per_s))
        + "; "
        + verdict("p99 ms"; $tmpfs | spread(.loadtest.latency_ms.p99); $disk | spread(.loadtest.latency_ms.p99))
    ]
  + (if ($measured | length) == 0 then ["- no case produced a result"] else [] end)
  + [
      "",
      "Host: \(map(.host) | unique | join("; "))",
      "Docker: \(map(.docker) | unique | join("; "))",
      "Volumes: \(map(.docker_root_backing) | unique | join("; "))"
    ]
  + (if ($failed | length) == 0 then []
     else ["", "Failed cases:"] + [$failed[] | "- RF \(.rf), \(.data_mode): \(.outcome)"]
     end)
  | .[]
