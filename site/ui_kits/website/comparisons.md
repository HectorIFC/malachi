# Comparison claims: sources

Every statement about another system on the landing page (`index.html`, section "vs") is listed here with the
official documentation it was checked against on 2026-10-24. Anything that could not be confirmed in official
docs was left out; the list of removed claims is at the end. Product names are used only to identify the
systems compared. No logos. Malachi is not affiliated with, or endorsed by, any of them.

## Apache Kafka

- **K1. Partitions can be added, not removed.** "Kafka does not currently support reducing the number of
  partitions for a topic." Apache Kafka 4.3 docs, Basic Kafka Operations, Modifying topics:
  https://kafka.apache.org/43/operations/basic-kafka-operations/
- **K2. Adding partitions does not move existing data.** "when we do so we do not attempt to reorganize the
  data in the topic." Confluent Platform docs, Picking the number of partitions (the Apache Kafka operations
  text as published by Confluent): https://docs.confluent.io/platform/current/kafka/post-deployment.html
  The page words the consequence for keys as "a key's new records can go to a different partition"; that
  follows from K2 plus hash routing and is stated the same way in the Redpanda docs (R2, R3).
- **K3. Consumers checkpoint one offset per partition.** "we store one offset per partition". Same Confluent
  page as K2.
- **K4. Commit waits for the in-sync replica set.** "Kafka dynamically maintains a set of in-sync replicas
  (ISR)" and "A write to a Kafka partition is not considered committed until all in-sync replicas have
  received the write." Apache Kafka 4.3 docs, Design, Replicated Logs:
  https://kafka.apache.org/43/design/design/

## Redpanda

- **R1. Partitions cannot be reduced.** "You cannot reduce the number of partitions on an existing topic."
  Redpanda docs, Manage Topics: https://docs.redpanda.com/current/develop/config-topics/
- **R2. Added partitions do not receive existing records.** "the existing messages won’t be redistributed
  to the new partitions." Same page.
- **R3. Keyed records are placed by hashing the key.** "Redpanda assigns a record that has a key to a
  partition by hashing the key". Same page.

## Apache Pulsar

- **P1. Classic partitioned topics only grow.** "you can only increase the number of partitions." Apache
  Pulsar docs, Manage topics: https://pulsar.apache.org/docs/next/admin-api-topics/
- **P2. Resizing a classic partitioned topic can remap keys.** "changing the partition count can remap keys
  to different partitions." Apache Pulsar 5.0 docs, Scalable topics:
  https://pulsar.apache.org/docs/5.0.x/concepts-scalable-topics/
- **P3. Pulsar 5.0 scalable topics split and merge key ranges.** Same page: segments own key ranges, a
  per-topic controller splits hot segments and merges cold ones, and they need a client with the v5 API.
  Release status checked 2026-10-24: 5.0.0 is a GA LTS release dated 2026-10-05, "ready for production use",
  following the 5.0.0-M1 (2026-06-23, preview, not for production) and 5.0.0-M2 milestones. Today only the
  Java v5 client supports scalable topics. Source: Apache Pulsar blog, "Apache Pulsar 5.0 LTS: Scalable
  Topics and Faster Existing Workloads", https://pulsar.apache.org/blog/2026/10/05/announcing-apache-pulsar-5-0/
  ; download page listing 5.0 (LTS) released 05 Oct 2026: https://pulsar.apache.org/download/ . Because a GA
  release exists, the page does not say "in preview".
  The page says this plainly, because it removes the main difference the original Pulsar comparison relied on.
- **P4. A cluster runs brokers, BookKeeper bookies and a metadata store.** Apache Pulsar 4.0 docs,
  Architecture Overview: https://pulsar.apache.org/docs/4.0.x/concepts-architecture-overview/ ; Oxia is the
  recommended metadata store for new clusters, ZooKeeper is still supported: https://pulsar.apache.org/

## Malachi side (from the repository)

Split and merge are metadata operations between buddy ranges, with no segment copied; a key maps to the one
range that contains it; writes are acknowledged after a majority of copies fsync; metadata runs in Raft
groups on `ra` inside each node; clients hold opaque cursors. Sources: README.md and docs/ARCHITECTURE.md at
HectorIFC/malachi@main.

## Removed (not confirmed in official docs, or not provable)

- From the Kafka video: "Uma partição travando o cluster inteiro" (a single partition stalling the cluster);
  "Range dividido, offset volta a zero"; "Sem ISR para vigiar" (implies ISR is a burden: opinion).
- From the Redpanda video: "Um inteiro para o tópico inteiro" (no matching statement in the docs); "Carga
  desigual vira split, não incidente" (an outcome claim with no measurement behind it).
- From the Pulsar video: "Your clients just learned your storage layout" and "In Pulsar, partitions can be
  added but never removed" (true only for classic topics since Pulsar 5.0, see P3); "Same key, two
  partitions. user-42 was paid before it was created" (a dramatised example, replaced by the docs' wording
  in P2); "A node fails. The cluster repairs itself." and "Secure unless you opt out." (Malachi claims left
  to the guarantees section, worded from the repository).
