# Running the chaos drills

Three harnesses take a real 3-node RF=3 Docker cluster, inject real failures while synthetic traffic
runs, and certify that a set of invariants held. They are the repo's version of NorthGuard's
certification pipeline: not tests of code paths, but proof that the system as deployed survives the
things that actually happen to it.

They are worth the wall clock. Every one of them has found a bug that no unit test caught, because
the failures they inject are the ones that only exist once processes, disks and a network are real:
control-plane amnesia across restarts, cold reads answering `:eof`, a peer addressed by the wrong
registered name.

## What you need

Docker, and a Docker VM with at least 8 CPUs. The harnesses pin the three servers and the load
generator to **disjoint** cpusets (`SRV_CPUSET` defaults to `2-7`, `LT_CPUSET` to `0-1`) so the client
can never steal server CPU and make the numbers lie. Adjust both if your machine is smaller. `jq` is
required as well.

Each harness builds the cluster images if they are missing, runs to completion, tears the cluster
down, and exits non-zero if any invariant broke.

Run one harness at a time. They all bring up the same compose project with the same container names
(`malachi-cluster-1` to `-3`), as does `benchmark/docker-cluster.sh`, and each starts with
`docker compose down -v`. A harness therefore refuses to start while any `malachi-cluster-*` container
is running, and prints the command that removes a leftover. It also prints the host load when it starts:
a drill sharing the machine with a CPU-heavy job can fail for reasons that are not in the broker.

## Node faults

```bash
scripts/docker-chaos-test.sh
```

Four events, each followed by a wait for the cluster to reconverge:

- **power pull**: `docker kill` (SIGKILL) of node 3, then restart. No graceful shutdown, no flush.
- **network partition**: node 2 disconnected from the network, then reconnected.
- **stalled node**: `docker pause` (SIGSTOP) of node 1. Worse than a dead node, because its sockets
  stay open and accept connections while nothing behind them answers.
- **rolling restart** of all three.

Three invariants are certified. An acknowledged write is never lost: a checker produces sequential
values through the whole window, retries through the faults, records only **confirmed** writes, and
at the end proves every one of them is still readable. The cluster reconverges to 3/3 healthy after
every event. And a clean produce plus fetch passes once the chaos ends: errors *during* an event are
expected, errors after it are not.

`CHECKER_WINDOW_S` (default 150) sets how long the durability checker runs, and so roughly how long
the drill takes.

## Storage corruption

```bash
scripts/docker-storage-chaos.sh
```

Five kinds of damage, always to a **follower** copy, always injected with that node **stopped**. The
stopped part is not incidental: an early version injected damage into a live node and in-flight
pushes refilled a truncation back to full size before the restart, hiding the very hole the probe was
meant to find.

- **torn write**: a segment copy cut to three quarters with a garbage tail, the classic
  crash-mid-write shape.
- **truncation**: a copy cut to half.
- **file loss**: a sealed segment directory deleted outright. Metadata still says RF=3, so only a
  physical probe can notice.
- **bit rot**: bytes flipped *inside* a sealed copy, keeping the file's exact size. No size probe can
  see this one. The copy looks perfect and answers reads with the records before the damage and
  nothing after, silently. Only checksum verification catches it.
- **rotted index**: the sparse index sidecar corrupted while its records stay intact. The index is
  derived data, so the repair has to be local, rebuilt from the segment without consulting a peer.

On top of the node-fault invariants, this one certifies that the damaged copies physically
reconverge. The check (`copies` mode of `scripts/chaos_checker.exs`) reads each node's data volume
read-only and compares, per segment, what every copy holds:

- **the records**, byte for byte over the valid part of each file, concatenated in offset order;
- **the count**, which for a segment the control plane sealed must equal its sealed length;
- **the readability**, since a copy that fails verification, holds non-zero bytes past its valid end,
  or whose files do not chain (a file named for an offset its records do not start at, or one that does
  not start where the previous ended) is damaged whatever the other copies say.

It does not require the files themselves to be identical, and it used to. Healthy copies differ as
files on every run ([#152](https://github.com/HectorIFC/malachi/issues/152)). Only a fenced copy, usually
the primary's, has its preallocated tail trimmed, so a follower's last file keeps its blank tail. And each
node rolls its internal files at its own sync points. The report calls that `benign`. That recovery zeroes
a torn write inside the preallocated region, instead of truncating it, is pinned by the store's own tests.

The check retries for a minute and prints a `COPIES segments=... whole_file=... content=...` summary.
When it fails, it prints, per node, every segment whose copies are not identical: the files with their
sizes and md5s, the records and bytes that verify, a digest of the valid records, and a verdict naming
the nodes that disagree. It also keeps the evidence under `tmp/chaos/evidence/<time>-<commit>/`, in a
numbered directory per failed check (`01-repair`, `02-invariant-4`): the report, the host's substrate and
load, and each node's copy of those segments. That happens before the second phase, whose fresh cluster
deletes the volumes. The result JSON names the run's directory in `evidence_dir`. The repairs of the
file-loss and bit-rot events are judged by the same rule, and keep their evidence the same way when they
do not converge; the rotted index is judged by byte equality of the index files.

Set `STORAGE_CHAOS_NEGATIVE_CONTROL=1` to prove the comparison is not vacuous. After the check, the
drill stops a follower, inverts one byte inside the records of one of its sealed copies, and requires
the comparison to fail naming that node before the node comes back. Then it requires the integrity
scrub to repair the copy. Run it by hand whenever the comparison changes.

Then a second phase, on a fresh cluster of its own:

- **full volume**: one node's log directory is a small volume (a size-limited tmpfs, from
  `docker-compose.storage-full.yml`), filled to ENOSPC while the checker keeps producing. The node must
  stay up: no restart, no crash of the replication server, still healthy. The failures must actually
  happen (the node logs them, so the event cannot pass without injecting anything), and the segments
  whose copy failed must be sealed on the other two replicas so producers move to new ones. Space is
  freed at the end and the acked-durability and clean-produce invariants close the run.

It needs a cluster of its own because a tmpfs comes back empty whenever its container restarts, and
the corruption events restart followers: sharing one cluster would turn each of those restarts into
the loss of every copy on that node.

## Config deployment

```bash
scripts/docker-config-chaos.sh
```

Two events, with the durability checker producing through both:

- **rolling config deploy**: a harmless setting rolled node by node (recreate, wait healthy, next).
  Availability must hold between steps, all three must end healthy, and the new value must be
  effective everywhere.
- **bad config and rollback**: a config that fails fast at boot is deployed to **one** node. It must
  crash-loop and never go healthy, while the other two keep serving quorum writes; rolling the
  environment back must bring it home.

## Reshard durability

```bash
scripts/docker-reshard-restart-chaos.sh
```

Two events, with the durability checker producing through both:

- **grow the ring while serving**: the cluster boots with `MALACHI_LOG_VNODES=4` and
  `mix malachi.reshard --to 6` runs against it. The reshard must complete, the recorded ring must
  report six vnodes with two of them created by splitting, and acks must keep flowing through the
  metadata migrations.
- **full-cluster restart**: all three nodes are stopped **together**, so no node survives to gossip
  the ring, and then started again. The recorded ring must come back with exactly the same six
  vnodes, split-created ones included, even though `MALACHI_LOG_VNODES` still says four.

This is the drill an in-process test cannot stand in for: it takes away the operating-system
processes and every copy of the ring held in memory. Before the ring was durable, the second event
came back believing the environment and orphaned the metadata the reshard had moved.

> **Currently red, and not because of the ring.** The two shared closing invariants below (acked-write
> survival and the post-chaos produce) fail on this drill today: a sharded control plane returns from a
> full-cluster restart healthy but unable to serve metadata writes. That is
> [#136](https://github.com/HectorIFC/malachi/issues/136), which reproduces on `main`, reproduces with
> no reshard involved, and does not reproduce on an unsharded cluster. The two events above, which are
> what this drill certifies, pass.

## Recording a result

Set `CHAOS_RESULT_FILE` and the harness writes the whole run as JSON alongside its console output:
the certification name, the verdict, the replication factor, every fault injected, the measured
invariants, and the failures if any.

```bash
CHAOS_RESULT_FILE=/tmp/chaos.json scripts/docker-chaos-test.sh
```

The file is written **before** the harness exits, including when it failed, because a failed drill is
exactly the run whose record is worth keeping. Unset the variable and nothing is written.

The [Chaos certification results](../generated/chaos-results.md) page renders exactly this document,
from `benchmark/published/chaos-node.json`, and the [benchmark dashboard](https://hectorifc.github.io/malachi/benchmarks/) shows
the same run beside the two load tests.

CI keeps that file current: the Publish results workflow runs the node-fault drill on every push to
main and commits its record, failures included. Only that drill is published. The storage-corruption
drill also runs in CI (the Storage chaos certification workflow: on demand, weekly, and on pull requests
that touch storage, replication, repair or the drill). It is not a required check, and it publishes
nothing: its JSON and any evidence are uploaded as the `chaos-storage` artifact. The config-deployment
and reshard drills are run by hand, so a record from either stays wherever you point
`CHAOS_RESULT_FILE`.

## Reading a failure

On failure the harness dumps each node's recent error and warning lines **before** tearing the
cluster down. That ordering is deliberate: `docker compose down` removes the containers, and losing
the postmortem to the teardown once cost a full diagnosis round.

Every check reports through `fail` rather than exiting, so a run always reaches its summary and you
see every broken invariant in one pass instead of only the first.
