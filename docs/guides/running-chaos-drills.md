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
reconverge: byte-identical segment files across all three nodes.

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
from `benchmark/published/chaos-node.json`.

## Reading a failure

On failure the harness dumps each node's recent error and warning lines **before** tearing the
cluster down. That ordering is deliberate: `docker compose down` removes the containers, and losing
the postmortem to the teardown once cost a full diagnosis round.

Every check reports through `fail` rather than exiting, so a run always reaches its summary and you
see every broken invariant in one pass instead of only the first.
