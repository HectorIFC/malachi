# Introduction

Malachi is a **distributed log storage system** written in Elixir: an open-source reimplementation of the
architecture behind LinkedIn's NorthGuard, running on the BEAM.

It is a **log, not a queue**. Producers append records to a named, ordered, replicated log; consumers read
from a position they carry themselves. Nothing is destroyed on read, several consumers can read the same
data independently, and a consumer can rewind.

> #### Not production-ready {: .warning}
>
> Malachi is under active development. Interfaces and on-disk formats can still change, it has not been
> battle-tested at scale, and there is no stability guarantee yet. Use it to learn and experiment, not to
> run production workloads.

## ...

<iframe width="560" height="315" src="https://www.youtube.com/embed/u1a_Kxs1yeE?si=qRkTPMTerwWkatA9" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

## What you get

- **An append-only log per topic**, durable on disk (segments with CRC-checked records, fsync before ack)
  and replicated across nodes by **quorum**.
- **A client that never sees partitions or offsets.** You produce by *key* and consume with an **opaque
  cursor**. That indirection is the point: the broker can split, merge and restripe the underlying ranges
  while the cluster runs, without breaking any client.
- **A metadata control plane on Raft** (`ra`), optionally **sharded** across virtual nodes so metadata
  itself scales, with online vnode splitting and re-sharding.
- **Consumer groups** with server-side committed positions, plus **server-push streaming** with credit-based
  flow control.
- **Security that is on by default**: Argon2 password hashing, per-topic ACLs, TLS, and pluggable external
  authentication (mTLS identity, OIDC/JWT).

## Where to go next

| If you want to… | Read |
|---|---|
| Run it and append your first record | [Getting started](getting-started.md) |
| Understand topics, keys, ranges and cursors | [The log model](log-model.md) |
| See the full architecture and design decisions | [Architecture](../ARCHITECTURE.md) |
| Understand the auth architecture | [Auth and user management (ADR)](../AUTH_USER_MANAGEMENT.md) |

## Status

Malachi is under active development and **not yet ready for production use**. The log model, the Raft-backed
sharded control plane (including online vnode split and grow re-sharding), replication with quorum commit,
self-healing, primary failover, consumer groups, and the security stack are implemented and covered by
tests, but the project has not been battle-tested at scale and makes no stability or durability guarantee
yet: treat it as software to learn from and experiment with. The API reference in this site is generated
from the source, so it always matches the code you are reading.
