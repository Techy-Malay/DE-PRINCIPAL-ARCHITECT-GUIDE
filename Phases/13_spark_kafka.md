# DE Master Doc — Chunk 13: Spark Internals + Advanced Kafka
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 13.1 Spark Execution Model — Internals

### Theory

**Job → Stage → Task hierarchy:**
```
ACTION (count(), write(), collect())
  └── JOB (one per action)
        └── STAGES (split at shuffle boundaries)
              └── TASKS (one per partition, run on executor)

Example:
  df.filter(...).groupBy(...).agg(...).write.parquet(...)
  
  Job 1:
    Stage 1: filter + map (no shuffle) → N tasks (N = input partitions)
    [SHUFFLE BOUNDARY — groupBy requires data redistribution]
    Stage 2: aggregate (after shuffle) → M tasks (M = output partitions)
    Stage 3: write → M tasks
```

**Why shuffle is expensive:**
```
Before shuffle:  data on executors by partition order
After shuffle:   data must be on executor by GROUP KEY

Shuffle steps:
1. Map side: each task writes shuffle output files to local disk (sort by key)
2. Network: shuffle files transferred across executors
3. Reduce side: each task reads its assigned keys from all other executors

Cost: disk I/O (write) + network transfer + disk I/O (read)
      = most expensive operation in Spark

Shuffle trigger operations:
  groupBy, join (non-broadcast), distinct, repartition,
  orderBy (global sort), cogroup, intersection
```

**Spark memory model:**
```
Executor JVM Heap
├── Reserved Memory (300MB fixed — Spark internal)
├── User Memory (spark.memory.fraction complement)
│   └── User data structures, UDF state
└── Unified Memory (spark.memory.fraction, default 60%)
    ├── Execution Memory (shuffle, aggregation, sort)
    │   └── Can borrow from Storage if available
    └── Storage Memory (cache, broadcast variables)
        └── Can borrow from Execution if available

Off-heap Memory (spark.memory.offHeap.enabled):
  Avoids GC pressure. Better for large datasets.
  Configured separately: spark.memory.offHeap.size

Typical config (16GB executor):
  spark.executor.memory = 16g
  spark.executor.memoryOverhead = 2g  (off-heap, native)
  Reserved: 300MB
  Unified: (16GB - 300MB) × 0.6 ≈ 9.4GB
  User:    (16GB - 300MB) × 0.4 ≈ 6.3GB
```

**Tungsten execution engine:**
- Binary format: stores data off-heap in binary, avoids Java object overhead
- Whole-stage codegen: generates bytecode per query stage (no virtual dispatch)
- Vectorized reading: reads Parquet in columnar batches (not row by row)
- Cache-aware sorting: data layout optimized for CPU cache lines

**DAG visualization — what to look for in Spark UI:**
```
Spark UI → Jobs → Stages → Tasks

Green bar (skipped):   Stage result cached — re-use without re-compute
Long stage (red):      Bottleneck — investigate this stage
Task time variance:    High variance = data skew (some tasks 10x slower)
GC time > 10% total:  Memory pressure — tune executor memory
Shuffle read/write:    High values = shuffle bottleneck
Spill (memory/disk):  OOM risk — increase executor memory or reduce partition size
```

### Production Example — Diagnosing a Slow Spark Job
```python
# Step 1: Check Spark UI for skewed stage
# Tasks tab: if 1 task takes 10min while others take 30s → skew

# Step 2: Find the skewed key
df.groupBy("payer_id").count().orderBy(desc("count")).show(10)
# Output: BCBS payer has 80% of records → skewed join key

# Step 3: Fix with AQE (automatic)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
# AQE splits skewed partitions at runtime → balanced tasks

# Step 4: Fix with salting (manual, for extreme skew)
from pyspark.sql.functions import concat, lit, floor, rand

SALT_FACTOR = 50

# Salt the large fact table
df_claims_salted = df_claims.withColumn(
    "salt", (floor(rand() * SALT_FACTOR)).cast("string")
).withColumn("payer_id_salted", concat(col("payer_id"), lit("_"), col("salt")))

# Explode the small dim table to match all salts
from pyspark.sql.functions import array, explode
df_payer_salted = df_payer.withColumn(
    "salt_array", array([lit(str(i)) for i in range(SALT_FACTOR)])
).withColumn("salt", explode("salt_array")) \
 .withColumn("payer_id_salted", concat(col("payer_id"), lit("_"), col("salt")))

# Join on salted key (balanced distribution)
result = df_claims_salted.join(df_payer_salted, on="payer_id_salted") \
    .drop("salt", "payer_id_salted", "salt_array")
```

### repartition vs coalesce vs partitionBy
```python
# repartition(N): full shuffle → N partitions (balanced)
# Use: increase partitions, balance after filter, before join
df.repartition(200)                          # round-robin shuffle
df.repartition(200, col("payer_id"))         # hash partition by column

# coalesce(N): no shuffle → reduce partitions (merge local)
# Use: reduce partitions before write (avoid small files)
# Cannot increase partitions (no shuffle)
df.coalesce(50)                              # merge partitions locally, no network

# partitionBy (write): directory-level partitioning on disk
# NOT the same as repartition — controls output file structure
df.repartition(col("claim_year"), col("claim_month")) \
  .write.partitionBy("claim_year", "claim_month").parquet("s3://...")
# Each partition column = directory level
# Combine with repartition for controlled file size

# Rule:
# Increasing partitions     → repartition()
# Reducing before write     → coalesce()  (no shuffle cost)
# Output directory layout   → partitionBy() on write
```

### Serialization
```python
# Java serialization (default): slow, large
# Kryo serialization: 10x faster, smaller

spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", "false")

# Register custom classes for best Kryo performance
from pyspark import SparkConf
conf = SparkConf()
conf.set("spark.kryo.classesToRegister",
         "com.company.ClaimRecord,com.company.PatientRecord")

# When serialization matters:
# - Shuffle data (always serialized)
# - Broadcast variables (serialized on driver, sent to executors)
# - Cached DataFrames with StorageLevel.MEMORY_ONLY_SER
# - UDF closures
```

### Speculative Execution
```python
# Slow task detected → launch duplicate task on another executor
# First to finish wins, other killed
# Good for: stragglers due to bad hardware
# Bad for: non-idempotent tasks (double write side effects)

spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "3")  # task 3x slower than median → speculate
spark.conf.set("spark.speculation.quantile", "0.9")  # 90% tasks done before speculation starts

# Disable for non-idempotent operations
spark.conf.set("spark.speculation", "false")  # safe for Delta MERGE (idempotent)
```

### Common Mistakes
- ❌ `collect()` on large DataFrame — pulls all data to driver → OOM
- ❌ Python UDFs in PySpark — Java→Python serialization per row = 10-100x slower than native Spark functions
- ❌ Too many small partitions (1M partitions of 1KB) — task scheduling overhead dominates
- ❌ Too few large partitions (1 partition of 100GB) — no parallelism, single executor bottleneck
- ❌ Not persisting reused DataFrames — re-computes from scratch on every action
- ❌ `repartition` before every write — unnecessary shuffle; use `coalesce` to reduce
- ✅ Target partition size: 128MB–512MB per partition (tune with `spark.sql.shuffle.partitions`)
- ✅ Use native Spark SQL functions over Python UDFs wherever possible
- ✅ `spark.sql.shuffle.partitions` default is 200 — tune for your data size

### Interview Questions
1. What is the difference between a Spark Job, Stage, and Task?
2. Why is shuffle the most expensive operation in Spark?
3. Explain Spark's unified memory model — Execution vs Storage memory.
4. What is the difference between `repartition()` and `coalesce()`?
5. What is whole-stage codegen in Tungsten and what problem does it solve?
6. How does Adaptive Query Execution (AQE) improve Spark performance at runtime?
7. What causes data skew in Spark joins and how do you fix it?
8. When should you use Kryo serialization over Java serialization?

---

## 13.2 Advanced Kafka Internals

### Theory

**Log structure:**
```
Topic: claims-submitted
  └── Partition 0
  │     └── Segment files on disk:
  │           00000000000000000000.log   ← messages (binary, append-only)
  │           00000000000000000000.index ← offset → physical position map
  │           00000000000000000000.timeindex ← timestamp → offset map
  │           00000000000005000000.log   ← new segment (after size/time threshold)
  └── Partition 1
  └── Partition 2

Segment rotation triggers:
  log.segment.bytes (default 1GB) — rotate when segment reaches size
  log.roll.ms (default 7 days)   — rotate on time regardless of size

Retention policies:
  log.retention.hours=168        — delete segments older than 7 days
  log.retention.bytes=-1         — no size-based retention (default)
  log.cleanup.policy=delete      — delete old segments
  log.cleanup.policy=compact     — keep only latest value per key (compaction)
```

**Compacted topics:**
```
Regular topic:   keeps all messages within retention window
Compacted topic: keeps only the LATEST message per key (forever or until null)

Use case:
  - Database change log (latest state per record key)
  - Config store (latest config per service)
  - User profile updates (latest profile per user_id)

Tombstone record: message with key + NULL value
  → Compaction will DELETE all records for that key
  → Equivalent to "delete" in a KV store

Example flow:
  Produce: key=CLM-001, value={status: SUBMITTED}
  Produce: key=CLM-001, value={status: APPROVED}   ← compaction keeps this
  Produce: key=CLM-001, value=null                  ← tombstone: deletes CLM-001

Config:
  log.cleanup.policy=compact
  min.cleanable.dirty.ratio=0.5   ← compact when 50% of log is dirty (re-keyed)
  delete.retention.ms=86400000    ← tombstones retained 24h (consumers can see delete)
```

**Consumer group rebalance:**
```
Eager rebalance (default, pre-2.4):
  1. All consumers stop consuming (STOP THE WORLD)
  2. All revoke all partitions
  3. Coordinator re-assigns partitions
  4. All consumers restart
  Problem: full stop = high latency spike, re-processing risk

Cooperative (Incremental) rebalance (2.4+, recommended):
  1. Coordinator identifies partitions to move
  2. Only affected partitions revoked
  3. Revoked partitions re-assigned to new owner
  4. Unaffected consumers never stop
  Result: minimal disruption during rebalance

Config:
  partition.assignment.strategy=
    org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```

**Partition assignment strategies:**
| Strategy | How | Best For |
|---|---|---|
| RangeAssignor | Partition ranges per topic | Simple, predictable |
| RoundRobinAssignor | Round-robin across consumers | Balanced load |
| StickyAssignor | Minimize moves on rebalance | Stateful consumers |
| CooperativeStickyAssignor | Sticky + incremental rebalance | **Production default** |

**ISR (In-Sync Replicas) deep dive:**
```
ISR = set of replicas fully caught up to leader

acks=0:   Producer doesn't wait. Fire and forget. Risk: data loss.
acks=1:   Leader acknowledges. Risk: data loss if leader fails before replication.
acks=all: All ISR replicas acknowledge. No data loss (if ISR > 1).
          = acks=-1 (equivalent)

min.insync.replicas=2 (critical config):
  With replication.factor=3, min.insync.replicas=2:
  Producer with acks=all → requires 2 replicas in ISR
  If only 1 replica in ISR → NotEnoughReplicasException
  → Prevents data loss even if leader and one follower fail

Unclean leader election:
  unclean.leader.election.enable=false (production default)
  → Only ISR replicas can become leader
  → Prevents out-of-ISR replica (stale) becoming leader (data loss)
  
  unclean.leader.election.enable=true
  → Any replica can become leader
  → Risk: stale replica misses messages → data loss
  → Only for availability-over-durability use cases
```

**Kafka Transactions (Exactly-Once):**
```python
# Producer: transactional writes
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    enable_idempotence=True,                # dedup by sequence number
    transactional_id='claims-producer-001', # unique per producer instance
    acks='all',
    retries=2147483647,
    max_in_flight_requests_per_connection=5
)

producer.init_transactions()

try:
    producer.begin_transaction()
    producer.send('claims-validated', key=b'CLM-001', value=payload)
    producer.send('claims-audit', key=b'CLM-001', value=audit_payload)
    # Both sends atomic — either both committed or both aborted
    producer.commit_transaction()
except KafkaException as e:
    producer.abort_transaction()

# Consumer: read only committed messages
consumer = KafkaConsumer(
    'claims-validated',
    isolation_level='read_committed',  # skip uncommitted/aborted messages
    bootstrap_servers='broker:9092',
    group_id='claims-processor'
)
```

**MirrorMaker 2 (cross-cluster replication):**
```yaml
# mm2.properties — replicate from us-east to us-west (DR)
clusters = us-east, us-west
us-east.bootstrap.servers = east-broker:9092
us-west.bootstrap.servers = west-broker:9092

# Replication: us-east → us-west
us-east->us-west.enabled = true
us-east->us-west.topics = claims-submitted, claims-validated, claims-flagged

# Topic naming: us-west gets "us-east.claims-submitted" (prefixed)
# Prevents circular replication

# Offset sync: consumer offsets translated between clusters
# On failover: consumer can resume from correct offset on us-west

# Replication lag monitoring:
# kafka.consumer.group.lag metric per mirrored topic
```

**Kafka Connect — architecture + SMTs:**
```
Kafka Connect architecture:
  Workers (distributed mode): stateless, scalable
  Connectors: plugins defining source/sink logic
  Tasks: actual parallel workers per connector
  Offset storage: Kafka topic (connect-offsets)
  Config storage: Kafka topic (connect-configs)
  Status storage: Kafka topic (connect-status)

Single Message Transforms (SMTs):
  Applied to each message in the connector pipeline
  
  Common SMTs:
  - ExtractField: pull nested field to top level
  - ReplaceField: rename/drop fields
  - MaskField: replace value with null/zero (PII masking)
  - TimestampConverter: convert date formats
  - Filter: drop messages matching a predicate
  - Flatten: flatten nested structs

# Example: Debezium connector with SMTs
{
  "transforms": "unwrap,maskSSN",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.maskSSN.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.maskSSN.fields": "ssn",
  "transforms.maskSSN.replacement": "***-**-****"
}
```

**Kafka Streams vs Flink:**
| Dimension | Kafka Streams | Apache Flink |
|---|---|---|
| Deployment | Library (no cluster) | Cluster required |
| State store | RocksDB (local, backed by changelog topic) | RocksDB / heap |
| Exactly-once | Yes (transactions) | Yes (checkpoints + 2PC) |
| Windowing | Tumbling, Hopping, Session | Full (incl. custom) |
| SQL | KSQL / ksqlDB | Flink SQL (richer) |
| Scalability | Good (partition-based) | Excellent |
| Best for | Kafka-native, simple topology | Complex stateful, large-scale |
| Latency | Milliseconds | Milliseconds |

### Common Mistakes
- ❌ `auto.offset.reset=earliest` in production — consumer restart replays all history
- ❌ Too few partitions — can't scale consumers beyond partition count
- ❌ Too many partitions — high metadata overhead, slow leader election
- ❌ `unclean.leader.election.enable=true` in financial systems — data loss risk
- ❌ Not setting `min.insync.replicas=2` with `acks=all` — false sense of durability
- ❌ Ignoring consumer group lag — silent pipeline delay
- ✅ Rule: partitions ≥ max expected consumers. Start with 12-24 for most topics.
- ✅ Monitor: consumer group lag, under-replicated partitions, ISR shrink events

### Interview Questions
1. What is a Kafka log segment and when does it rotate?
2. What is a compacted topic and when would you use it over a regular topic?
3. What is the difference between eager and cooperative rebalance in Kafka consumers?
4. What is ISR and how do `acks=all` + `min.insync.replicas` work together?
5. What is an unclean leader election and when should it be disabled?
6. How do Kafka transactions achieve exactly-once semantics?
7. What is MirrorMaker 2 and how does it handle consumer offset translation on failover?
8. What is a Kafka Connect SMT and give two production use cases?
9. When would you choose Kafka Streams over Apache Flink?

---

## 13.3 Spark Streaming — Windows Deep Dive

### Theory

**Event time vs Processing time:**
```
Event time:      When the event actually occurred (in the data)
Processing time: When Spark/Flink processes the event (wall clock)

Difference matters when:
  - Events arrive out of order
  - Network delays cause late arrival
  - Reprocessing historical data (event time = past, processing time = now)

Always use EVENT TIME for business windows (daily sales, hourly counts)
Processing time windows are only valid for monitoring/ops metrics
```

**Window types:**

**1. Tumbling Window (Fixed, non-overlapping)**
```python
# Non-overlapping, fixed-size windows
# Each event belongs to exactly ONE window
# Example: hourly claim count

df_windowed = df_stream \
    .withWatermark("claim_event_time", "10 minutes") \
    .groupBy(
        window(col("claim_event_time"), "1 hour"),  # 1-hour tumbling
        col("payer_id")
    ).agg(count("claim_id").alias("claim_count"),
          sum("billed_amount").alias("total_billed"))

# Windows: [00:00-01:00), [01:00-02:00), [02:00-03:00)
# Event at 00:45 → goes into [00:00-01:00) only
```

**2. Sliding Window (Overlapping)**
```python
# Overlapping windows. Each event may belong to MULTIPLE windows.
# Example: 1-hour window, sliding every 15 minutes
# Use: rolling averages, moving totals

df_sliding = df_stream \
    .withWatermark("claim_event_time", "30 minutes") \
    .groupBy(
        window(col("claim_event_time"), "1 hour", "15 minutes"),  # size, slide
        col("payer_id")
    ).agg(avg("processing_days").alias("rolling_avg_tat"))

# Windows: [00:00-01:00), [00:15-01:15), [00:30-01:30), [00:45-01:45)
# Event at 00:45 → belongs to ALL FOUR windows above
# Cost: more state to maintain (4x vs tumbling)
```

**3. Session Window (Gap-based, dynamic)**
```python
# Window closes after period of inactivity (gap)
# Window size is dynamic — grows while events keep arriving
# Example: patient activity session, user click session

# Flink SQL (native session window support)
spark.sql("""
    SELECT
        patient_id,
        SESSION_START(event_time, INTERVAL '30' MINUTE) AS session_start,
        SESSION_END(event_time, INTERVAL '30' MINUTE)   AS session_end,
        COUNT(*) AS events_in_session
    FROM patient_events
    GROUP BY patient_id,
             SESSION(event_time, INTERVAL '30' MINUTE)
""")
# Session: patient active at 10:00, 10:15, 10:25 → one session (< 30min gap)
# New session: patient active again at 11:30 → new session (> 30min gap)
```

**Watermarks — handling late data:**
```python
# Watermark = how late can events arrive and still be included?
# Spark tracks: max(event_time seen so far) - watermark_delay = watermark threshold
# Events with event_time < watermark threshold → DROPPED (too late)

df.withWatermark("event_time", "10 minutes")
# If max event_time seen = 14:30
# Watermark threshold = 14:30 - 10min = 14:20
# Events with event_time < 14:20 → dropped
# Events with event_time >= 14:20 → included in windows

# Window finalizes when: watermark > window_end_time
# [14:00-14:30) window finalizes when watermark passes 14:30
# = when Spark sees event_time > 14:40 (14:40 - 10min watermark = 14:30)

# Tradeoff:
# Small watermark (1 min): low latency, drops more late events
# Large watermark (1 hour): high latency, handles more late events, more state
```

**Output modes for streaming:**
```python
# Complete: output entire aggregated result every trigger
# Use: small result sets (global counts), not scalable for large state
df.writeStream.outputMode("complete")

# Append: output only NEW rows added to result
# Use: non-aggregated streams, no-update aggregations
df.writeStream.outputMode("append")

# Update: output only CHANGED rows in result
# Use: aggregations with watermark (most common for windowed aggregations)
df.writeStream.outputMode("update")

# Rule: aggregation with watermark → "update" mode
#        non-aggregated → "append" mode
#        full result needed every trigger → "complete" mode
```

### Common Mistakes
- ❌ No watermark on windowed aggregations — state grows unboundedly → OOM
- ❌ Processing time windows for business metrics — wrong results on replay/reprocessing
- ❌ Watermark too small — legitimate late events dropped, data loss
- ❌ Watermark too large — high memory state, high latency before window finalizes
- ❌ `complete` output mode on large aggregation state — writes full result every trigger
- ✅ Always set watermark before window aggregation
- ✅ Use `update` output mode with windowed aggregations + watermark

### Interview Questions
1. What is the difference between tumbling, sliding, and session windows?
2. What is a watermark in Spark Structured Streaming and how does it determine when a window finalizes?
3. When would you use `update` output mode vs `append` vs `complete`?
4. What is the trade-off between watermark size and latency?
5. Why should you always use event time rather than processing time for business windows?

---

*Next: Chunk 14 — dbt Advanced Patterns + Data Contracts + Quick Reference*
