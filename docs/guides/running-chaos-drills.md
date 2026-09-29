# Running the chaos drills

Five harnesses take a real 3-node RF=3 Docker cluster, inject real failures while synthetic traffic
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

The check retries for a minute and prints a
`COPIES segments=... whole_file=... content=... control=...` summary, where `control` says whether the
topology was read; a report with `control=unavailable` never certifies.
When it fails, it prints, per node, every segment whose copies are not identical: the files with their
sizes and md5s, the records and bytes that verify, a digest of the valid records, and a verdict naming
the nodes that disagree. A disagreement in record counts is named by its direction, because the two
directions are different defects: `behind` is a copy short of what the segment holds, which is data this
replica lost or has not caught up on, while `ahead` is a copy holding records its segment's seal excludes
([#175](https://github.com/HectorIFC/malachi/issues/175)). `record_count_split` is the case where the
named copies differ both ways at once, or where a two-copy tie leaves nothing to compare against. When
the control plane's sealed length is known it is what the direction is measured from, not what the peers
happen to hold. It also keeps the evidence under `tmp/chaos/evidence/<time>-<commit>/`, in a
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

## Rolling upgrade and rollback

```bash
scripts/docker-upgrade-chaos.sh
OLD_REF=v0.14.2 scripts/docker-upgrade-chaos.sh
```

The one operation a release has to survive before anyone relies on it: replacing the binary node by
node under load, and then putting the old one back. The drill builds two images. **OLD** is a release,
built from its own tree: `OLD_REF`, or by default the release an operator would be upgrading from. Every
merge is tagged, so when your tree's code (`lib`, `config`, `mix.lock`) is the newest release's, as it is
on `main`, that is the newest release whose code differs (a release that only changed the docs is
skipped); when your tree changes the code, it is the newest release. The oldest it accepts is 0.14.2
for an unsharded run (the first release with every gate the canary exercises and every control-plane
group this tree starts) and 0.16.1 for a sharded one (see below), although no release runs sharded before
the first one that carries the fix for [#136](https://github.com/HectorIFC/malachi/issues/136). The run prints which release it took and why, and records it.

**NEW** is your working tree with `test/support/upgrade_canary.patch` applied, which gives it what no release has yet: a capability and a cluster flag (`upgrade_canary`), a metadata
command at the next machine version, a replication message older builds do not know, and a data format
the flag raises the directories to. Nothing of the canary ships; on every pull request the patch is applied
to the tree and the result compiled, so it never falls behind the code it patches.

Three phases, each with its own checker window and topic, and every node keeping its volume across
every swap:

- **roll forward under the pin**: `MALACHI_RA_MACHINE_VERSION` holds the control plane at OLD's version,
  as the [operations guide](operations.md#the-control-plane-machine-version) tells an operator to, and
  each node goes from OLD to NEW. Halfway through, with node 3 on NEW and the others on OLD, the flag
  must be refused naming the OLD nodes, the canary command must be refused and leave the topic metadata
  identical on all three replicas and out of the NEW node's own metadata cache, and an OLD node must have
  counted the unknown replication message instead of crashing on it. On a sharded run (below), with all
  three on NEW the metadata ring is split from four vnodes to five.
- **roll back before the flip**: every node goes back to OLD, which on a sharded run has to bring up the
  vnode the split created. This must succeed.
- **roll forward, finalize, flip, refused rollback**: forward again under the pin, then the pin is
  removed node by node, which moves the control plane to the new machine version, where the canary
  command applies. The flag is switched on, which raises every data directory to format 2. Node 3 is
  then started on OLD: it must exit 78 with the format marker's refusal, its data directory (the segment
  files and the control plane's state beside them) must be byte for byte what it was, and the other two
  must keep serving. It then goes back to NEW.

The control plane is sharded (four vnodes, split to five) only when OLD itself brings a restarted node's
vnode members back, which releases before the fix for
[#136](https://github.com/HectorIFC/malachi/issues/136) do not: rolled back onto such a release, a sharded
cluster loses each vnode's quorum at its second node, whatever the drill does. On an older OLD the drill runs
unsharded and splits nothing. The run prints which one it chose and why, and records it.

Every swap is certified three ways: acks kept flowing while the node was down (the node is stopped first,
and the count is polled while it stays stopped, before it is recreated), acks kept flowing once it was
back, and a produce through that node alone was clean. Every phase ends with the acked-durability
check and a comparison of every Raft group across its members at equal applied indexes. The run ends by
searching the saved log of every container it replaced for a crash report.

`OLD_PATCH` and `NEW_PATCH` apply one more patch to either tree. They exist for the runs that show the
drill catches what it claims (revert a guard and watch it fail); a result recorded with either is marked
`negative_control`, so it can never be mistaken for a certification.

A run takes 40 to 55 minutes: the three checker windows alone are 29 (each phase waits out its window),
on top of three image builds and fourteen node swaps. `PHASE1_WINDOW_S`, `PHASE2_WINDOW_S` and
`PHASE3_WINDOW_S` size the windows; a phase that outlasts its window fails and names the one to raise.
A run that cannot start (a patch that no longer applies, an image that does not build, another cluster
already running) still writes its result, as failed, and removes the images it built.

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
main and commits its record, failures included. The Rolling upgrade certification workflow runs the
upgrade drill every night and on demand, and publishes the nightly record the same way to
`benchmark/published/chaos-upgrade.json`, rendered as the
[Rolling upgrade certification results](../generated/chaos-upgrade-results.md) page. No other drill is
published. The storage-corruption
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
