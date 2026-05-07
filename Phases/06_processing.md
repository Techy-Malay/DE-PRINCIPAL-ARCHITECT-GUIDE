# DE Master Doc — Chunk 06: Processing
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 6.1 ETL vs ELT

### Theory

**ETL (Extract → Transform → Load)**
- Transform data BEFORE loading into target.
- Transformation happens in a dedicated engine (Informatica, SSIS, Talend, Spark).
- Target receives clean, structured data.
- Historical pattern: on-prem DWH with limited storage (storage was expensive).
- Still dominant with: Informatica IICS, PowerCenter, SSIS, Talend.

**ELT (Extract → Load → Transform)**
- Load raw data into target FIRST, then transform inside the target engine.
- Transformation happens IN the DWH/Lakehouse (Snowflake, BigQuery, Databricks).
- Raw data preserved in landing/bronze zone.
- Modern cloud pattern: storage is cheap, compute is elastic and powerful.
- Tools: dbt (transformation layer), Fivetran/Airbyte (extract+load), Spark SQL.

**Why ELT won in the cloud era:**
- Cloud DWH compute (Snowflake, BigQuery) is powerful enough to transform at scale
- Raw data preservation enables reprocessing without re-extracting from source
- dbt made SQL-based transformation engineering-grade (version control, testing, lineage)
- No separate transformation server to manage

### ETL vs ELT Decision Guide
```
Use ETL when:
  • PII/sensitive data must be masked BEFORE entering any storage
  • Source system is too slow to re-extract (ETL runs once, transforms en route)
  • On-prem DWH with expensive storage (can't afford to land raw)
  • Complex transformations not expressible in SQL (custom Python logic)
  • Informatica/PowerCenter already licensed and operating

Use ELT when:
  • Cloud DWH (Snowflake, BigQuery, Databricks) is target
  • Raw data preservation is required (audit, reprocessing)
  • dbt is the transformation layer
  • Schema evolution is frequent (raw lands first, transform evolves)
  • Team is SQL-proficient, not Java/Scala-heavy
```

### Production Example — Informatica IICS (ETL) → dbt (ELT)
```
ETL path (Informatica IICS):
  Source (Oracle EHR)
    → IICS Pipeline (extract + mask PII + validate + transform)
    → Snowflake staging table (clean, masked)
    → dbt models (dimensional modeling in Snowflake)

ELT path (Fivetran + dbt):
  Source (SaaS CRM)
    → Fivetran (extract + load raw to Snowflake RAW schema)
    → dbt (transform raw → staging → marts in Snowflake)
    → BI tools (Power BI / Tableau)
```

```sql
-- dbt model: staging layer (light transform)
-- models/staging/stg_claims.sql
SELECT
    claim_id                              AS claim_nk,
    patient_id                            AS patient_nk,
    CAST(claim_date AS DATE)              AS claim_date,
    UPPER(TRIM(status_code))              AS status_code,
    COALESCE(billed_amount, 0)            AS billed_amount,
    _fivetran_synced                      AS load_timestamp
FROM {{ source('raw', 'claims') }}
WHERE claim_id IS NOT NULL

-- dbt model: mart layer (business logic)
-- models/marts/fact_claims.sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['c.claim_nk']) }} AS claim_sk,
    p.patient_sk,
    pr.provider_sk,
    py.payer_sk,
    d.date_sk                             AS service_date_sk,
    c.billed_amount,
    c.paid_amount,
    DATEDIFF('day', c.claim_date, c.paid_date) AS processing_days
FROM {{ ref('stg_claims') }}        c
JOIN {{ ref('dim_patient') }}       p  ON c.patient_nk  = p.patient_nk
JOIN {{ ref('dim_provider') }}      pr ON c.provider_nk = pr.provider_nk
JOIN {{ ref('dim_payer') }}         py ON c.payer_nk    = py.payer_nk
JOIN {{ ref('dim_date') }}          d  ON c.claim_date  = d.full_date
```

### Tradeoffs
| Dimension | ETL | ELT |
|---|---|---|
| Raw data preservation | No (transformed in flight) | Yes (raw always available) |
| PII handling | Mask before landing | Requires masking policy in DWH |
| Reprocessing | Re-extract from source | Re-transform from raw |
| Storage cost | Lower (only clean data) | Higher (raw + clean) |
| Flexibility | Lower (fixed transform logic) | Higher (evolve transforms without re-extract) |
| Tooling | Informatica, SSIS, Talend | dbt, Spark SQL, BigQuery |
| Cloud fit | Legacy | Native |

### Common Mistakes
- ❌ ELT without masking policies — PII lands in raw layer unprotected
- ❌ ETL without raw archive — when transform logic is wrong, must re-extract from source (which may be gone)
- ❌ dbt without tests — ELT data quality relies on dbt tests; skipping them = silent bad data in marts
- ❌ Mixing ETL and ELT responsibilities in one pipeline — creates ambiguous ownership
- ✅ Always land raw first (ELT pattern) even in ETL shops — archive raw for replay
- ✅ dbt: source freshness tests + not_null + unique + relationship tests on every model

### Interview Questions
1. Why did ELT replace ETL as the dominant pattern in cloud data engineering?
2. When would you still choose ETL over ELT in 2024?
3. How does dbt fit into the ELT pattern architecturally?
4. What happens when transform logic is wrong in ETL vs ELT — how do you recover?
5. How do you handle PII in an ELT pipeline where raw data lands first?

---

## 6.2 Batch vs Streaming

### Theory

**Batch Processing**
- Process data in discrete chunks (hourly, daily, weekly).
- High throughput. Optimized for large volumes processed together.
- Simpler to implement, debug, and reprocess.
- Latency: minutes to hours (acceptable for most BI/reporting).
- Tools: Spark batch, dbt, Hive, BigQuery scheduled queries.

**Streaming Processing**
- Process data continuously as events arrive.
- Low latency: milliseconds to seconds.
- Stateful processing (windows, aggregations over time).
- More complex: ordering, late data, exactly-once, state management.
- Tools: Apache Flink, Spark Structured Streaming, Kafka Streams, Dataflow.

**Micro-batch (middle ground)**
- Process small batches on a tight schedule (seconds interval).
- Spark Structured Streaming default mode.
- Simpler than true streaming; latency: seconds.
- Good for: near-real-time dashboards, CDC landing.

### When to Choose
```
Batch when:
  • Latency > 15 minutes is acceptable
  • Complex aggregations over large historical datasets
  • ML model training
  • Regulatory reports (daily/monthly)
  • Cost optimization is critical (batch is cheaper)

Streaming when:
  • Real-time fraud detection (< 1 second)
  • Live dashboards (< 5 seconds)
  • Event-driven pipelines (trigger on event arrival)
  • CDC propagation to downstream systems
  • IoT sensor processing

Micro-batch when:
  • Near-real-time BI (< 30 seconds acceptable)
  • Kafka → Delta Lake landing
  • Simpler than true streaming, lower latency than batch
```

### Production Example
```python
# Spark Structured Streaming — Kafka → Delta Lake (micro-batch)
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, DecimalType, DateType

spark = SparkSession.builder.appName("ClaimsStreaming").getOrCreate()

schema = StructType() \
    .add("claim_id", StringType()) \
    .add("patient_id", StringType()) \
    .add("amount", DecimalType(12,2)) \
    .add("claim_date", DateType())

# Read from Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "claims-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON payload
df_parsed = df_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_ts", current_timestamp())

# Write to Delta Lake (micro-batch, 30-second trigger)
df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://checkpoints/claims/") \
    .trigger(processingTime="30 seconds") \
    .start("s3://datalake/bronze/claims/")
```

### Tradeoffs
| Dimension | Batch | Micro-batch | Streaming |
|---|---|---|---|
| Latency | Hours/minutes | Seconds | Milliseconds |
| Throughput | Very high | High | Medium |
| Complexity | Low | Medium | High |
| Cost | Lower | Medium | Higher |
| Reprocessing | Simple (re-run job) | Simple (replay offsets) | Complex (state rebuild) |
| Late data handling | Easy (window is fixed) | Manageable | Complex (watermarks) |
| Best for | BI, reporting, ML training | Near-RT dashboards, CDC | Fraud, IoT, alerts |

### Common Mistakes
- ❌ Streaming when batch is sufficient — streaming is expensive and complex for no reason
- ❌ No checkpoint location in Spark Structured Streaming — job restart = reprocess from scratch
- ❌ No watermark for late data — streaming state grows unboundedly → OOM
- ❌ Trigger interval too short — overhead of job scheduling dominates compute time
- ✅ Default to batch. Only move to streaming when latency requirement cannot be met.
- ✅ Always set watermarks: `.withWatermark("event_time", "10 minutes")`

### Interview Questions
1. What is the difference between micro-batch and true streaming in Spark?
2. When would you choose streaming over batch for a claims processing pipeline?
3. What is a watermark in stream processing and why is it required?
4. How do you reprocess historical data in a streaming pipeline?
5. What is backpressure in streaming and how does Flink handle it?

---

## 6.3 CDC — Change Data Capture Internals

### Theory

**What CDC solves:** Capture only changed rows from source systems without full table scans. Essential for real-time data synchronization, streaming pipelines, and Lakehouse ingestion.

**CDC Methods:**

**1. Log-based CDC (Gold standard)**
- Reads database transaction log (WAL in PostgreSQL, Redo Log in Oracle, Binary Log in MySQL).
- Zero impact on source system (no queries, no triggers).
- Captures INSERT, UPDATE, DELETE with before/after images.
- Tools: **Debezium** (open source), Oracle GoldenGate, AWS DMS, Attunity.
- Latency: milliseconds.

**2. Query-based CDC**
- Polls source table on `updated_at` timestamp: `WHERE updated_at > last_run_time`.
- Misses hard DELETEs (no timestamp on deleted rows).
- Source system impact: runs queries on production DB.
- Simpler to implement. Works when log access is unavailable.

**3. Trigger-based CDC**
- DB trigger fires on INSERT/UPDATE/DELETE → writes to shadow/audit table.
- High source system overhead. Triggers slow every write.
- Legacy pattern. Avoid in modern architectures.

**4. Timestamp + Watermark**
- Combination: use `updated_at` + sequence numbers to track position.
- Better than pure query-based but still misses hard DELETEs.

### Log-based CDC Deep Dive (Debezium + Kafka)
```
PostgreSQL WAL (Write-Ahead Log)
         │
         ▼
  Debezium Connector (Kafka Connect)
  • Reads WAL via logical replication slot
  • Converts WAL entries to Kafka messages
  • Message format: {before: {...}, after: {...}, op: "u/i/d"}
         │
         ▼
  Kafka Topic: postgres.public.claims
  • op: "c" = INSERT (before: null, after: {row})
  • op: "u" = UPDATE (before: {old}, after: {new})
  • op: "d" = DELETE (before: {row}, after: null)
  • op: "r" = READ (snapshot / initial load)
         │
         ▼
  Flink / Spark Streaming Consumer
  • Apply MERGE/UPSERT to Delta/Iceberg target
  • Track by primary key
         │
         ▼
  Delta Lake / Snowflake (target)
```

### Debezium Message Structure
```json
{
  "before": {
    "claim_id": "CLM-001",
    "status": "SUBMITTED",
    "amount": 1500.00
  },
  "after": {
    "claim_id": "CLM-001",
    "status": "APPROVED",
    "amount": 1500.00
  },
  "op": "u",
  "ts_ms": 1706745600000,
  "source": {
    "db": "claims_db",
    "table": "claims",
    "lsn": 12345678
  }
}
```

### Applying CDC to Delta Lake
```python
# Flink / Spark: apply CDC events as MERGE to Delta target
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "s3://datalake/silver/claims/")

# Process CDC batch
def upsert_to_delta(micro_batch_df, batch_id):
    # Separate operations
    inserts_updates = micro_batch_df.filter("op IN ('c', 'u', 'r')")
    deletes = micro_batch_df.filter("op = 'd'")

    # MERGE for inserts/updates
    (delta_table.alias("target")
        .merge(
            inserts_updates.select("after.*").alias("source"),
            "target.claim_id = source.claim_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    # Hard delete handling
    if deletes.count() > 0:
        delete_keys = deletes.select("before.claim_id").collect()
        # Option 1: physical delete
        # Option 2: soft delete (set is_deleted = TRUE)

df_cdc.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "s3://checkpoints/claims-cdc/") \
    .start()
```

### Tradeoffs — CDC Methods
| Method | Source Impact | Captures DELETEs | Latency | Complexity |
|---|---|---|---|---|
| Log-based (Debezium) | None | Yes | Milliseconds | High |
| Query-based | Medium (DB queries) | No | Minutes | Low |
| Trigger-based | High (per-row trigger) | Yes | Seconds | Medium |
| Timestamp watermark | Low | No | Minutes | Low |

### Common Mistakes
- ❌ Query-based CDC on tables without `updated_at` index — full table scan every poll
- ❌ Log-based CDC without monitoring replication slot lag — WAL accumulates, fills disk
- ❌ Not handling DELETE events — target table grows forever, never reflects source deletes
- ❌ Applying CDC without idempotency — network retry = duplicate row in target
- ❌ Snapshot (initial load) not handled separately — treating "r" ops same as "c" ops
- ✅ Always monitor Debezium connector lag (Kafka consumer group lag)
- ✅ Use `before` image for DELETE, `after` image for INSERT/UPDATE

### Interview Questions
1. What is the difference between log-based and query-based CDC?
2. How does Debezium capture changes without impacting the source database?
3. Why does query-based CDC miss hard DELETEs?
4. What is a PostgreSQL replication slot and why must you monitor its lag?
5. How do you handle the initial snapshot (full load) in a Debezium CDC pipeline?
6. How do you apply CDC events idempotently to a Delta Lake target?

---

## 6.4 Exactly-Once Semantics

### Theory

Three delivery guarantees in distributed messaging:

| Guarantee | Definition | Risk | Example |
|---|---|---|---|
| At-most-once | Message delivered 0 or 1 times | Data loss | Fire-and-forget logs |
| At-least-once | Message delivered 1 or more times | Duplicates | Default Kafka consumer |
| Exactly-once | Message delivered exactly once | None (hardest) | Financial transactions |

**Exactly-once is a combination of:**
1. **Exactly-once delivery:** Message infrastructure guarantees no duplicates in transit.
2. **Idempotent processing:** Processing the same message twice produces the same result.

**Kafka exactly-once (transactions API):**
```
Producer → idempotent producer (dedup by sequence number within epoch)
         → transactional producer (atomic write across partitions)
Consumer → read_committed isolation (only see committed messages)
         → atomic consume + process + produce (Kafka Streams)
```

**Flink exactly-once:**
- Chandy-Lamport algorithm: distributed snapshots (checkpoints).
- Barrier injection: checkpoint barriers flow through the DAG.
- All operators snapshot state when barrier arrives.
- On failure: restore from last complete checkpoint.
- Sink must support two-phase commit (2PC) or idempotent writes.

**Spark Structured Streaming exactly-once:**
- Checkpoint + WAL (Write-Ahead Log) for source offsets.
- Idempotent sinks (Delta Lake: transactional writes).
- Micro-batch: each batch has deterministic output for given input.

### Production Example — Kafka Exactly-Once
```python
# Kafka producer: idempotent + transactional
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'broker:9092',
    'enable.idempotence': True,           # dedup by sequence number
    'transactional.id': 'claims-producer-1',
    'acks': 'all',
    'retries': 2147483647,
    'max.in.flight.requests.per.connection': 5
})

producer.init_transactions()

try:
    producer.begin_transaction()
    producer.produce('claims-topic', key='CLM-001', value=payload)
    producer.commit_transaction()        # atomic commit
except Exception as e:
    producer.abort_transaction()         # rollback on failure
```

### Common Mistakes
- ❌ Confusing exactly-once delivery with idempotent processing — you need BOTH
- ❌ Using exactly-once Kafka transactions without read_committed on consumer — consumer sees uncommitted messages
- ❌ Non-idempotent sink (plain INSERT) with exactly-once source — duplicates on retry
- ✅ Delta Lake is an idempotent sink: transactional writes prevent duplicate commits on retry
- ✅ Design sinks to be idempotent first; exactly-once delivery is the second layer

### Interview Questions
1. What is the difference between at-least-once and exactly-once semantics?
2. How does Kafka's idempotent producer prevent duplicates?
3. How does Flink achieve exactly-once with its checkpoint mechanism?
4. Why is an idempotent sink required even with exactly-once source delivery?
5. What is two-phase commit (2PC) and how does Flink use it for exactly-once sinks?

---

## 6.5 Idempotency

### Theory

**Idempotency:** An operation produces the same result regardless of how many times it is executed.

Critical in distributed systems because: network failures, retries, and restarts cause the same message or job to execute multiple times. Idempotent processing means retries are safe.

**Idempotency patterns:**

**1. Deduplication key**
Assign a unique ID to each message/record. Target system deduplicates on this key.
```sql
-- Snowflake: dedup on insert
INSERT INTO claims
SELECT * FROM staging_claims s
WHERE NOT EXISTS (
    SELECT 1 FROM claims c WHERE c.claim_id = s.claim_id
);

-- Or: MERGE (upsert pattern — idempotent)
MERGE INTO claims t
USING staging_claims s ON t.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**2. Deterministic output**
Same input always produces same output. No random IDs, no `NOW()` in transformations.
```python
# ❌ Non-idempotent: generates new UUID on every run
df = df.withColumn("record_id", expr("uuid()"))

# ✅ Idempotent: deterministic hash from business key
df = df.withColumn("record_id", md5(col("claim_id")))
```

**3. Idempotent write (Delta Lake)**
Delta's transaction log prevents duplicate commits. Writing the same batch twice with same data = same result (no duplicates in table).

**4. Offset tracking**
Track last processed offset/watermark. On restart, resume from last committed position.
```python
# Spark Structured Streaming: checkpoint = idempotent restart
df.writeStream \
    .option("checkpointLocation", "s3://checkpoints/claims/") \
    .start()
# On restart: reads checkpoint → resumes from last committed offset
# Same data will not be reprocessed (Kafka offsets committed atomically)
```

### Common Mistakes
- ❌ Using `INSERT` without dedup check — retries insert duplicates
- ❌ `SELECT NOW()` or `CURRENT_TIMESTAMP()` in transformation — different timestamp on retry = different row
- ❌ Generating surrogate keys with AUTOINCREMENT in streaming — retry generates different SK
- ❌ No checkpoint in Spark Streaming — restart reprocesses from beginning
- ✅ MERGE/UPSERT is the idempotent alternative to INSERT for streaming pipelines
- ✅ Hash keys (MD5/SHA) for record IDs in streaming — deterministic, retry-safe

### Interview Questions
1. What is idempotency and why is it critical in distributed data pipelines?
2. How do you make a Spark Structured Streaming job idempotent on restart?
3. Why is `MERGE` preferred over `INSERT` in CDC/streaming pipelines?
4. What makes a hash key idempotent while an AUTOINCREMENT key is not?

---

## 6.6 Retry Strategies

### Theory

Failures in distributed pipelines are inevitable. Retry strategy determines how the system recovers.

**Retry patterns:**

| Pattern | Behavior | Use Case |
|---|---|---|
| Immediate retry | Retry instantly | Transient network blip |
| Fixed delay | Wait N seconds between retries | Rate-limited APIs |
| Exponential backoff | Wait doubles each retry (1s, 2s, 4s, 8s...) | Overloaded downstream |
| Exponential backoff + jitter | Backoff + random delay | Prevent thundering herd |
| Dead Letter Queue (DLQ) | After N retries, move to DLQ | Poison pill messages |
| Circuit breaker | Stop retrying when failure rate > threshold | Cascading failure prevention |

**Exponential backoff + jitter (best practice):**
```python
import time, random

def retry_with_backoff(fn, max_retries=5, base_delay=1.0):
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt)        # exponential
            jitter = random.uniform(0, delay * 0.1)    # 10% jitter
            time.sleep(delay + jitter)
```

**Airflow retry config:**
```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60),
}
```

**Dead Letter Queue pattern:**
```
Kafka Consumer → process message
                     │
                     ├── Success → commit offset
                     └── Failure (after N retries)
                               → produce to DLQ topic
                               → alert + manual investigation
                               → commit original offset (don't block pipeline)
```

### Common Mistakes
- ❌ Immediate retry on all errors — retrying a bad message instantly = tight loop, floods downstream
- ❌ No DLQ — one poison pill message blocks entire partition forever
- ❌ Unlimited retries — pipeline hangs indefinitely on unrecoverable errors
- ❌ Retry without idempotency — retries cause duplicates in target
- ✅ Always pair retry with idempotent processing
- ✅ DLQ + alerting is mandatory for production streaming pipelines

### Interview Questions
1. What is exponential backoff with jitter and why is jitter important?
2. What is a Dead Letter Queue and when should a message be sent there?
3. What is a circuit breaker pattern and how does it prevent cascading failures?
4. How do you ensure retries don't create duplicate records in the target?

---

## 6.7 Backpressure

### Theory

**Backpressure:** Mechanism by which a slow consumer signals upstream producers to slow down, preventing memory overflow and data loss.

**Why it happens:**
- Producer generates data faster than consumer can process
- Downstream bottleneck (slow DB write, network congestion)
- Spike in event volume

**How systems handle it:**

**Kafka:** Consumer naturally applies backpressure by controlling poll rate. Producer never overwhelms consumer — consumer pulls at its own pace. Kafka acts as buffer. Backpressure = simply not polling.

**Flink:** Credit-based flow control. Each operator tracks buffer credits. Upstream operator only sends data when downstream has buffer credits available. Propagates back through the entire DAG. No data loss, no OOM — pipeline slows gracefully.

**Spark Structured Streaming:** `maxOffsetsPerTrigger` limits records per micro-batch. Consumer naturally bounded. Kafka lag increases (buffer) but Spark doesn't OOM.

```python
# Spark: limit consumption rate to prevent OOM
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "claims-topic") \
    .option("maxOffsetsPerTrigger", 10000) \  # backpressure limit
    .load()

# Flink: credit-based flow control is automatic
# Monitor: network buffer utilization in Flink UI
# Alert when: output buffer pool usage > 80% consistently
```

**Backpressure signals to monitor:**
```
Kafka consumer lag            → consumer falling behind producer
Flink: checkpoint duration ↑  → system under pressure, state saving slower
Flink: busy time = 100%       → operator is bottleneck
Spark: trigger interval > processingTime → batch taking longer than trigger
```

### Common Mistakes
- ❌ No rate limiting on Kafka consumer — spike event causes OOM crash
- ❌ Ignoring Kafka consumer lag — lag is the primary backpressure signal
- ❌ Scaling out without identifying bottleneck operator — wrong operator gets more resources
- ✅ Monitor Flink busy time per operator to find the bottleneck before scaling
- ✅ `maxOffsetsPerTrigger` is your first lever for Spark streaming backpressure

### Interview Questions
1. What is backpressure and why is it important in streaming pipelines?
2. How does Flink's credit-based flow control work?
3. What metrics indicate backpressure in a Kafka + Spark Streaming pipeline?
4. How does Kafka's pull model naturally implement backpressure?

---

## 6.8 Event-Driven Architecture

### Theory

**Event-Driven Architecture (EDA):** System components communicate via events (immutable records of something that happened) rather than direct API calls.

**Core concepts:**
- **Event:** Immutable fact. "ClaimSubmitted", "PaymentProcessed", "PatientAdmitted". Has timestamp, source, payload.
- **Event Producer:** Emits events without knowing who consumes them (decoupled).
- **Event Consumer:** Subscribes to events, processes independently.
- **Event Broker:** Kafka, Kinesis, Pub/Sub. Durable, ordered, replayable log.
- **Event Schema:** Avro/Protobuf with Schema Registry. Enforces contract.

**EDA vs Request-Response:**
```
Request-Response (REST/RPC):
  Service A → HTTP POST → Service B (synchronous, coupled)
  Service A waits for response. If B is down, A fails.

Event-Driven:
  Service A → Kafka → Service B (asynchronous, decoupled)
  Service A fires and forgets. B processes when ready.
  B can be down; events accumulate in Kafka. No data loss.
```

**Event sourcing pattern:**
Store all state changes as an immutable sequence of events. Current state = replay of all events. Enables: complete audit trail, time travel, event replay for new consumers.

```
Event Store (Kafka / EventStore):
  [ClaimCreated] → [ClaimReviewed] → [ClaimApproved] → [PaymentInitiated]
  
Current state = fold/reduce over event sequence
Replay from event 0 → rebuild any historical state
New consumer = replay from beginning = no ETL needed
```

### Production Example — Claims Event Pipeline
```
EHR System
    │ ClaimSubmitted event
    ▼
Kafka Topic: claims.submitted
    │
    ├──► Claims Processor (Flink)
    │         → validate, enrich
    │         → emit ClaimValidated / ClaimRejected
    │
    ├──► Audit Service (consumer)
    │         → write to audit log (DV2 Raw Vault)
    │
    └──► Real-time Dashboard (consumer)
              → update live claim status view
```

### Common Mistakes
- ❌ Events that are commands, not facts ("ProcessClaim" vs "ClaimSubmitted") — commands are coupled, facts are decoupled
- ❌ No schema registry — schema changes break all consumers silently
- ❌ Mutable events — events must be immutable; corrections are new events
- ❌ Giant event payloads — events should carry minimal context; consumers fetch details if needed
- ✅ Event names: past tense ("ClaimApproved", "PatientDischarged") = immutable facts
- ✅ Schema Registry + Avro = contract enforcement across all producers and consumers

### Interview Questions
1. What is the difference between an event and a command in EDA?
2. What is event sourcing and how does it differ from traditional state storage?
3. How does EDA improve system resilience compared to REST-based integration?
4. What is the role of Schema Registry in event-driven pipelines?

---

*Next: Chunk 07 — Orchestration (DAG Concepts, Airflow Internals, Scheduling, Dependency Management, Failure Recovery)*
