Worked through step by step, so the arithmetic is visible. These are the results under the stated premises
(10 thousand machines, 17 PB/day, 32 trillion records/day):

## Part 1: Data per second per machine

**Calculation:**
- 17 PB/day = 17 × 10¹⁵ bytes/day
- Divided by 86,400 seconds/day = **196.76 GB/s aggregate** across the whole fleet
- Divided by 10,000 machines = **~19.68 MB/s per machine** (or ~18.76 MiB/s in binary units)

To get a feel for the scale: each machine handles roughly **20 MB per second continuously, 24 hours a day,
without a break**. In an hour that is ~71 GB per machine. In a day, ~1.7 TB per machine.

## Part 2: Records per second per machine

**Calculation:**
- 32 trillion records/day ÷ 86,400 seconds = **370.37 million records/s aggregate**
- Divided by 10,000 machines = **~37,037 records/s per machine**

## Part 3: Average record size (an interesting derived number)

As a bonus, dividing total volume by record count:

- 17 × 10¹⁵ bytes ÷ 32 × 10¹² records = **~531 bytes per record** on average

That is consistent with what you would expect from Kafka/NorthGuard messages in a social-network and event
context: small payloads, most likely serialized JSON, or Avro/Protobuf with metadata (ids, timestamps,
event type, a few context fields). It matches the typical industry pattern.

## Summary table

| Metric | Aggregate | Per machine |
|---|---|---|
| Data/second | 196.76 GB/s | ~19.68 MB/s |
| Records/second | 370.37 million | ~37,037 |
| Data/day | 17 PB | ~1.7 TB |
| Records/day | 32 trillion | ~3.2 billion |

## Honest caveats

A few points, so these numbers are not over-read:

**1. Average is not peak.** Streaming systems see peaks of 3x to 10x the average at busy hours. Real peak
per machine could reach **60 to 200 MB/s** and **100k+ records/s**.

**2. Not every machine does the same work.** Among the 10 thousand machines there are brokers (which
dominate the I/O), coordinators, controllers, storage nodes and so on. Looking at active brokers alone, the
per-machine throughput is higher.

**3. Replication multiplies bytes.** Kafka usually replicates each message 3 times. If the 17 PB/day
already counts replication, the logical throughput (the part that serves the product) is lower. If it is
original production only, the physical bytes written to disk are 3 to 5 times larger. LinkedIn did not make
clear which is which in its public communication.

**4. An average size hides the distribution.** 531 bytes on average most likely has a long tail: many
messages of 100 to 200 bytes (simple logs, heartbeats) and some of several KB (rich event payloads, ML
features).
