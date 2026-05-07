# DE Master Doc — Chunk 01: Fundamentals
> Format per topic: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 1.1 Database vs DWH vs Data Lake vs Lakehouse

### Theory
| System | Storage Model | Schema | ACID | Primary Workload |
|---|---|---|---|---|
| OLTP Database | Row-store | Write-time | Full | High-freq reads/writes (ms) |
| Data Warehouse | Column-store | Write-time | Partial | Analytical queries (seconds-min) |
| Data Lake | Object storage | Read-time | None | ML, raw exploration |
| Lakehouse | Column on object store | Both | Full (via format) | BI + ML unified |

**OLTP Database:** Normalized to 3NF/BCNF. B-tree indexes for single-row lookup. WAL for durability. Examples: PostgreSQL, MySQL, Oracle. Designed for thousands of transactions/second.

**Data Warehouse:** Column-store enables vectorized execution — reads only queried columns. Compression ratios 5-10x vs row-store. Denormalized (star schema). Examples: Snowflake, Redshift, BigQuery. Designed for scanning billions of rows on a few columns.

**Data Lake:** Cheap object storage (S3, ADLS, GCS). Raw data in native format. Schema applied at read time — risk: "data swamps" (data without governance). Best for ML features, unstructured data, data science exploration.

**Lakehouse:** Applies DWH-grade governance on top of lake storage via open table formats (Delta Lake, Iceberg, Hudi). One copy of data serves BI + ML + streaming. Eliminates two-tier ETL complexity. ACID on S3.

### Architecture Diagram
```
OLTP Sources (Oracle/Postgres)
     │
     ├──[ETL]──► Data Warehouse (Snowflake) ──► BI / Reports
     │
     ├──[raw]──► Data Lake (S3/ADLS) ──► ML / Data Science
     │
     └──[unified]──► LAKEHOUSE (Delta/Iceberg)
                         ├── Bronze (Raw)
                         ├── Silver (Cleansed)
                         └── Gold (Business) ──► BI + ML + API
```

### Production Example (BFSI)
- Core banking → **Oracle 19c** (OLTP, strict ACID)
- Daily regulatory batch → **Snowflake DWH** via Informatica IICS
- Raw logs, clickstream, ML features → **S3 Data Lake** (Parquet)
- Fraud ML + regulatory reporting unified → **Delta Lake on Databricks** (Lakehouse)

### Tradeoffs
| Dimension | Database | DWH | Lake | Lakehouse |
|---|---|---|---|---|
| Cost | Medium | High | Low | Low-Medium |
| ACID | Full | Partial | None | Full |
| ML Support | Poor | Limited | Excellent | Excellent |
| BI Performance | Poor | Excellent | Moderate | Excellent |
| Schema Governance | Strong | Strong | Weak | Strong |

### Common Mistakes
- ❌ Running analytical queries on OLTP — causes row lock contention, kills transaction throughput
- ❌ Treating a Data Lake as a DWH substitute — without governance it becomes a swamp
- ❌ Building a Lakehouse without metadata catalog (Glue/Unity Catalog) — you get lake chaos
- ✅ Migration path: Start DWH → add Lake for ML → consolidate into Lakehouse

### Interview Questions
1. Why would you choose Lakehouse over maintaining separate Lake + DWH?
2. What is schema-on-read and what governance risks does it introduce?
3. How does Delta Lake achieve ACID transactions on top of S3?
4. When would you NOT consolidate into a Lakehouse? (Answer: regulatory data isolation, legacy BI dependency, Snowflake-only shop)

---

## 1.2 OLTP vs OLAP

### Theory

**OLTP (Online Transaction Processing)**
- Storage: Row-store. Full row read on every access.
- Access pattern: Single-row lookup via primary key / index.
- Optimization: B-tree indexes, buffer pool, WAL.
- Metric: Transactions per second (TPS).
- Normalization: 3NF — minimize write amplification.

**OLAP (Online Analytical Processing)**
- Storage: Column-store. Only queried columns read from disk.
- Access pattern: Full-column scans, GROUP BY, aggregations.
- Optimization: Vectorized execution, compression (RLE, dictionary), predicate pushdown.
- Metric: Query latency on billions of rows.
- Normalization: Denormalized (star schema) — minimize JOINs.

**Why column-store beats row-store for analytics:**
Scanning 1B rows for 3 columns — column-store reads only those 3 columns. Row-store reads entire rows (all 50+ columns). Column values compress far better (similar values adjacent): a `status` column with 3 values compresses 100x via RLE.

**HTAP (Hybrid Transactional/Analytical Processing):**
Single engine for both workloads. Examples: TiDB, CockroachDB, SingleStore. Eliminates ETL latency for real-time analytics. Trade-off: neither best-in-class OLTP nor OLAP performance. Right for: fraud detection on live transactions, live dashboards on operational data.

### Production SQL Example
```sql
-- OLTP: claims transaction (row-store, milliseconds)
INSERT INTO claims (claim_id, patient_id, amount, status)
VALUES ('CLM-2024-001', 'PAT-123', 1500.00, 'SUBMITTED');

-- OLAP: aggregation (column-store, seconds on billions of rows)
SELECT payer_name, diagnosis_code,
       SUM(amount)           AS total_claims,
       AVG(processing_days)  AS avg_tat
FROM   fact_claims
JOIN   dim_payer USING (payer_key)
WHERE  claim_year = 2024
GROUP  BY 1, 2
ORDER  BY total_claims DESC;
```

### Tradeoffs
| Dimension | OLTP | OLAP | HTAP |
|---|---|---|---|
| Storage | Row | Column | Both |
| Write performance | Excellent | Poor | Good |
| Scan performance | Poor | Excellent | Good |
| Data freshness | Real-time | T-1/hours | Real-time |
| Cost | Medium | Medium-High | High |
| Normalization | 3NF | Star/Snowflake | Both |

### Common Mistakes
- ❌ Running OLAP queries on OLTP — full table scan + lock contention brings down production
- ❌ Choosing HTAP thinking it will match dedicated OLTP/OLAP — it won't; it's a compromise
- ❌ Storing pre-aggregated results in OLTP tables to speed up dashboards — use a proper DWH
- ✅ Read replicas bridge the gap for low-complexity analytical needs on OLTP data

### Interview Questions
1. Why does column compression improve OLAP performance beyond just IO reduction?
2. What is vectorized execution and why does it matter for analytical engines?
3. When would you justify HTAP over separate OLTP + DWH?
4. Explain why an analytical query on OLTP causes row-level lock contention.

---

## 1.3 CAP Theorem

### Theory
A distributed system can guarantee **at most 2 of 3** simultaneously:

- **C — Consistency:** Every read returns the most recent write or an error. All nodes see identical data.
- **A — Availability:** Every request receives a response (not necessarily latest data). System never rejects.
- **P — Partition Tolerance:** System operates despite network partitions (nodes can't communicate).

**Critical insight:** In real distributed systems, network partitions are inevitable — P is non-negotiable. The real choice is **C vs A** when a partition occurs.

**CP Systems:** HBase, ZooKeeper, MongoDB (default). Return error during partition. Right for: financial balances, inventory, booking systems.

**AP Systems:** Cassandra, DynamoDB (default), CouchDB. Return stale data during partition. Right for: social feeds, shopping carts, DNS resolution.

**PACELC extension (more realistic):**
Even **without partition (E=else)**, there's a Latency (L) vs Consistency (C) trade-off. Most real systems tune this dynamically (Cassandra: `ONE` vs `QUORUM` vs `ALL`).

### Production Example
```
BFSI — Account Balance → CP required
  Bank must be consistent. $500 withdrawal must reflect everywhere.
  Cassandra (AP) is wrong here — stale read → double spend.
  Use: PostgreSQL (CP), Google Spanner (external consistency CP).

E-commerce — Shopping Cart → AP acceptable
  Amazon's cart can show stale items during partition.
  Dynamo paper (2007): user experience (availability) > stale cart.
  Use: DynamoDB (AP, eventual consistency).
```

### Tradeoffs
| System | CAP Choice | When to use |
|---|---|---|
| PostgreSQL | CP | Financial data, inventory |
| Cassandra | AP (tunable) | IoT, time-series, high write |
| MongoDB | CP (default) | Documents, flexible schema |
| DynamoDB | AP (default) | High-scale, cart, session |
| Spanner | CP (global) | Global financial, multi-region consistent |
| ZooKeeper | CP | Leader election, config management |

### Common Mistakes
- ❌ Treating CAP as static — Cassandra's consistency is tunable per operation
- ❌ Ignoring PACELC — latency/consistency trade-off exists even without failures
- ❌ Using AP system (Cassandra) for financial transactions without compensating transactions
- ❌ Applying CAP to Snowflake/BigQuery — CAP applies to distributed transactional stores, not analytical engines
- ✅ Configure Cassandra at `QUORUM` read+write for stronger consistency at cost of availability

### Interview Questions
1. Why is P always required in a real distributed system?
2. How does Cassandra let you tune between C and A?
3. Where does Snowflake sit in the CAP triangle and why?
4. Explain eventual consistency with a concrete example.
5. What is the PACELC model and why is it more practical than CAP?

---

## 1.4 Distributed Systems Basics

### Theory

**Sharding (Horizontal Partitioning)**
Data split across nodes. Each shard holds a subset.

| Strategy | How | Pro | Con |
|---|---|---|---|
| Range sharding | A-M → Shard 1, N-Z → Shard 2 | Simple, range queries efficient | Hot spots on sequential keys |
| Hash sharding | `shard = hash(key) % N` | Uniform distribution | Range queries hit all shards |
| Consistent hashing | Virtual ring, nodes claim ranges | Add/remove nodes = minimal reshuffling | More complex |

**Replication**
Copies of data on multiple nodes for fault tolerance and read scaling.

- **Synchronous:** Write acknowledged only after all replicas confirm. Strong consistency, higher latency.
- **Asynchronous:** Write acknowledged immediately, replicas catch up. Low latency, risk of data loss on failure.
- **Semi-synchronous:** Acknowledge after ≥1 replica (MySQL default).

**Consensus (Raft / Paxos)**
Algorithm for distributed nodes to agree on a single value. Raft requires majority quorum (n/2 + 1). Used in: etcd (Kubernetes), CockroachDB, TiKV. Critical for leader election and log replication.

**Coordinator Patterns**
- **Master-slave:** Single writer. Simple, no conflict resolution. Single point of failure.
- **Multi-master:** Multiple writers. Requires conflict resolution (last-write-wins, CRDTs).
- **Leaderless (Dynamo-style):** Any node accepts writes. Version vectors or CRDTs for conflict resolution.

### Production Example — Snowflake's Distributed Design
```
Snowflake = "Shared-Disk" architecture (NOT shared-nothing):
  - Storage layer: S3 (shared across all compute nodes)
  - Compute layer: Virtual Warehouses (ephemeral, no data ownership)
  - Coordination: Cloud Services Layer (metadata, query optimization)

Implication: Multiple VWs can read same data simultaneously (no
contention). VW adds nodes = faster scan, not more data access.
Local SSD on each VW node caches remote S3 reads → performance.
```

**Kafka's partition model (sharding + replication):**
```
Topic → N partitions (shards, horizontal scale)
Each partition → 1 leader + N-1 followers (replication)
Leader handles all reads/writes for that partition
ISR (In-Sync Replicas): set of replicas caught up to leader
acks=all: wait for all ISR replicas → no data loss guarantee
```

### Common Mistakes
- ❌ Hash sharding on time-series data — queries like "last 7 days" hit all shards
- ❌ Using synchronous replication across geographically distant nodes — latency kills throughput
- ❌ Single master with no failover — SPOF in production
- ✅ Consistent hashing is why Cassandra can add nodes with minimal data reshuffling (virtual nodes/vnodes)

### Interview Questions
1. What is consistent hashing and why does it minimize reshuffling when nodes are added?
2. Explain how Snowflake achieves compute/storage separation and its trade-offs.
3. What is a split-brain scenario and how does Raft prevent it?
4. When would you choose leaderless replication over leader-based?
5. What is the difference between ISR and OSR in Kafka and why does it matter for `acks=all`?

---

*Next: Chunk 02 — Storage & File Formats (Parquet/ORC/Avro, Delta/Iceberg/Hudi, Partitioning/Clustering/Z-order, Compression, Small File Problem)*
# DE Master Doc — Chunk 02: Storage & File Formats
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 2.1 Parquet vs ORC vs Avro

### Theory

**Apache Parquet**
- Columnar format. Data organized by column, then row group (default 128MB).
- Each column has statistics: min, max, null count, distinct count → predicate pushdown.
- Encodings: dictionary, RLE (Run-Length Encoding), bit-packing, delta.
- Schema embedded in file footer. Self-describing.
- Best for: analytical queries, read-heavy, wide tables, Spark/Hive/Presto/Athena/Snowflake external.

**Apache ORC (Optimized Row Columnar)**
- Also columnar. Hive-native format.
- Organized into stripes (~250MB). Each stripe has row data + column indexes + bloom filters.
- Bloom filters enable fast point lookups within stripes.
- Better compression than Parquet on integer-heavy columns (Zlib/Zstd).
- Best for: Hive workloads, Hive ACID tables, HBase integration.

**Apache Avro**
- Row-based format. Schema-first (JSON schema stored in file header).
- Excellent schema evolution: add fields with defaults = backward + forward compatible.
- Each record self-contained → natural fit for streaming (one message = one record).
- Best for: Kafka messages (with Schema Registry), data serialization, write-heavy, schema-evolution-heavy.
- NOT for analytics — full row scan required, no column pruning.

### When to Use Each
| Use Case | Format |
|---|---|
| Kafka messages | Avro (+ Schema Registry) |
| S3 data lake analytics | Parquet |
| Hive ACID tables | ORC |
| Snowflake external stage | Parquet |
| Spark batch analytics | Parquet |
| CDC source serialization | Avro |
| ML feature storage | Parquet |

### Production Example
```python
# Kafka → Avro (schema-safe serialization)
# Producer serializes with Schema Registry — evolution safe

# Spark: Land raw Avro from Kafka, convert to Parquet for analytics
df_avro = spark.read.format("avro").load("s3://raw/claims/")

df_avro.write \
    .format("parquet") \
    .partitionBy("year", "month", "day") \
    .option("compression", "snappy") \
    .mode("append") \
    .save("s3://bronze/claims/")

# Parquet predicate pushdown — skips row groups automatically
spark.read.parquet("s3://bronze/claims/") \
    .filter("claim_year = 2024 AND status = 'DENIED'")
# Spark reads column stats → skips row groups where max(claim_year) < 2024
```

### Tradeoffs
| Feature | Parquet | ORC | Avro |
|---|---|---|---|
| Layout | Columnar | Columnar | Row |
| Read (analytics) | Excellent | Excellent | Poor |
| Write speed | Good | Good | Excellent |
| Schema evolution | Limited | Limited | Excellent |
| Streaming fit | Poor | Poor | Excellent |
| Predicate pushdown | Row group stats | Stripe + bloom filter | None |
| Ecosystem | Universal | Hadoop-centric | Kafka-centric |
| Compression ratio | Good | Better (integers) | Good |

### Common Mistakes
- ❌ Using Avro for S3 analytics — no column pruning, full scan every time
- ❌ Parquet row group size too small → many small row groups → metadata overhead, poor pushdown granularity
- ❌ Parquet row group size too large → poor pruning granularity (must read more than needed)
- ❌ Forgetting Schema Registry with Avro in Kafka → schema embedded in every message → bloat
- ✅ Target: 128MB–512MB per Parquet file, ~1M rows per row group
- ✅ Snappy compression for Parquet (fast decompress), Zstd for better ratio where CPU is not bottleneck

### Interview Questions
1. How does predicate pushdown work in Parquet and what are its limits?
2. Why is Avro preferred over Parquet for Kafka messages?
3. What is a row group in Parquet and how do its statistics enable query optimization?
4. Compare Parquet row group stats vs ORC bloom filters — when does each help?
5. Why can't you use predicate pushdown on non-partitioned, non-clustered Parquet files?

---

## 2.2 Delta Lake vs Apache Iceberg vs Apache Hudi

### Theory

**Problem they solve:** Raw Parquet on S3 has no ACID. Multiple concurrent writers corrupt data. No schema enforcement at write time. No time travel. No efficient upserts. Open table formats add a metadata layer that provides DWH-grade guarantees on cheap object storage.

---

**Delta Lake (Databricks → Linux Foundation)**
- Transaction log: JSON commit files in `_delta_log/`. Each commit = one atomic operation.
- Optimistic concurrency control: writers check for conflicts at commit time.
- Time travel: replay transaction log to any version (`VERSION AS OF`, `TIMESTAMP AS OF`).
- Auto-compaction + OPTIMIZE command for small file merging.
- Z-order clustering for multi-dimensional data locality.
- Photon engine (Databricks-native vectorized execution).
- Best ecosystem: Databricks. Growing support: Spark, Trino, Athena.

**Apache Iceberg (Netflix + Apple → Apache)**
- Metadata hierarchy: catalog → table metadata → snapshot → manifest list → manifest → data files.
- Snapshot isolation: each write creates new snapshot. Readers see consistent snapshot.
- Hidden partitioning: partition spec stored in metadata, not in directory structure. Partition evolution without rewriting data.
- Column-level statistics at manifest level → aggressive file pruning.
- Best multi-engine support: Spark, Flink, Trino, Athena, Snowflake, Dremio.
- Truly open — no vendor lock-in.

**Apache Hudi (Uber → Apache)**
- Two table types:
  - **COW (Copy-on-Write):** Rewrites entire file on update. Slower writes, fast reads. Best for: read-heavy, batch updates.
  - **MOR (Merge-on-Read):** Appends delta logs on write, merges on read. Fast writes, slightly slower reads. Best for: high-frequency upserts, streaming ingestion.
- Record-level indexing: Bloom index, HBase index, Bucket index → fast upsert lookup.
- Built-in incremental pull for CDC consumers.
- Best for: CDC pipelines, streaming upserts, Uber-scale write-heavy workloads.

### Architecture Internals
```
DELTA LAKE:
s3://table/
├── _delta_log/
│   ├── 00000000000000000000.json   ← schema creation
│   ├── 00000000000000000001.json   ← add files
│   ├── 00000000000000000002.json   ← delete (remove files)
│   └── 00000000000000000010.checkpoint.parquet  ← compacted log
├── part-00001.snappy.parquet
└── part-00002.snappy.parquet

ICEBERG METADATA:
catalog (Glue / Nessie / REST)
  └── table_metadata.json  (points to current snapshot)
        └── snapshot-N.avro  (list of manifest files)
              └── manifest-N.avro  (file-level stats per data file)
                    └── data-00001.parquet

HUDI MOR WRITE PATH:
Write → Append to .log delta files (fast)
Read  → Merge base parquet + .log files at read time
Compaction → Periodic merge of .log into base parquet files
```

### Tradeoffs
| Feature | Delta Lake | Iceberg | Hudi |
|---|---|---|---|
| ACID | ✓ | ✓ | ✓ |
| Time Travel | ✓ (log replay) | ✓ (snapshots) | Limited |
| Schema Evolution | Good | Excellent | Good |
| Multi-engine | Growing | Excellent | Good |
| Upsert performance | Good | Good | Excellent (MOR) |
| Partition evolution | Manual | Hidden / automatic | Manual |
| Vendor | Databricks-leaning | Truly open | Apache/Uber |
| Best fit | Databricks shops | Multi-cloud/engine | CDC / streaming upserts |

### Production Decision Guide
```
On Databricks?               → Delta Lake (native, Photon, Unity Catalog)
Multi-cloud / multi-engine?  → Iceberg (Athena + Snowflake + Flink on same data)
CDC-heavy, high upsert rate? → Hudi MOR (record-level index, fastest upserts)
Snowflake external tables?   → Iceberg (native Snowflake Iceberg support)
```

### Common Mistakes
- ❌ Using Delta Lake when primary query engine is Trino/Presto (multi-engine support weaker)
- ❌ Using Hudi COW for streaming high-frequency upserts — every write rewrites files
- ❌ Forgetting to run `VACUUM` on Delta tables — old log files accumulate indefinitely
- ❌ Not setting `delta.logRetentionDuration` — time travel storage costs explode
- ✅ Run `OPTIMIZE` + `ZORDER BY` weekly on Delta tables to maintain query performance
- ✅ Iceberg's manifest-level stats prune more aggressively than Delta's file-level stats

### Interview Questions
1. How does Delta Lake's transaction log achieve ACID on S3?
2. What is Iceberg hidden partitioning and why is it superior to Hive-style partitioning?
3. Explain Hudi MOR vs COW with a write-heavy CDC use case.
4. How would you choose between Delta and Iceberg for a multi-cloud strategy?
5. What happens during a Delta `OPTIMIZE` operation under the hood?
6. How does Iceberg support partition evolution without rewriting data?

---

## 2.3 Partitioning vs Clustering vs Z-ordering

### Theory

**Partitioning**
Physical splitting of data files by column values into directories or logical segments.

- **Hive/Spark:** Directory-level. `s3://table/year=2024/month=01/day=15/`
- **Snowflake:** Implicit micro-partition pruning (no explicit directories). Metadata-driven.
- **BigQuery:** Column-level partitioning. Date/range/integer partitions.

Query with `WHERE year = 2024` skips all other partition directories entirely (partition pruning). Zero I/O on pruned partitions.

**Best cardinality:** Low-to-medium (date, region, status, country).
**Danger zone:** High-cardinality columns (patient_id, order_id) → millions of tiny files.

**Clustering (Snowflake)**
Re-sorts data within micro-partitions by cluster key to maximize min/max overlap reduction (pruning).

- Applied via `CLUSTER BY (col1, col2)` on Snowflake tables.
- Unlike Hive partitioning — no directory structure. Works within Snowflake's 50-500MB micro-partitions.
- Automatic Clustering: Snowflake background service continuously re-clusters as data grows (incremental credit cost).
- Best for: medium-to-high cardinality columns that appear in WHERE clauses.

**Z-ordering (Delta Lake / Databricks)**
Multi-dimensional space-filling curve that co-locates rows with similar values across multiple columns into the same files.

- `OPTIMIZE table ZORDER BY (region, product_category)`
- If queries frequently filter on (region, product_category), Z-ordering puts those combinations in adjacent files.
- Supports multi-column locality — unlike partitioning which is single-hierarchy.
- Bloom filters complement Z-ordering for point lookups on high-cardinality columns.
- Effectiveness diminishes beyond 3-4 columns.

### Decision Guide
```
Low cardinality column (date, region, status):
  → Partition by it (Hive/Spark/BigQuery) or cluster (Snowflake)

Medium-high cardinality column in WHERE clause:
  → Cluster (Snowflake) or Z-order (Delta)

Multi-column filter patterns (col_a AND col_b):
  → Z-order (Delta) or composite cluster key (Snowflake)

High cardinality point lookups (user_id, claim_id):
  → Bloom filter index (Delta/Hudi) or search optimization (Snowflake)
```

### Production Example
```sql
-- Snowflake: cluster on commonly filtered columns
ALTER TABLE fact_claims CLUSTER BY (claim_date, payer_id);

-- Monitor clustering quality
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_claims', '(claim_date, payer_id)');
-- Look for: average_depth close to 1.0 = well-clustered
-- average_depth > 6 = consider re-clustering

-- BigQuery: partition + cluster
CREATE TABLE claims_partitioned
PARTITION BY DATE(claim_date)
CLUSTER BY payer_id, diagnosis_code
AS SELECT * FROM raw_claims;
```

```python
# Delta Lake: partition + Z-order
df.write \
    .format("delta") \
    .partitionBy("year", "month") \      # low cardinality → partition
    .save("s3://datalake/claims/")

# Z-order on remaining filter columns
spark.sql("""
    OPTIMIZE delta.`s3://datalake/claims/`
    ZORDER BY (payer_id, diagnosis_code)
""")
# ❌ NEVER: partitionBy("patient_id") → millions of files
```

### Tradeoffs
| Technique | Cardinality | Multi-column | Engine | Cost |
|---|---|---|---|---|
| Hive Partitioning | Low only | Single hierarchy | Spark/Hive/Athena | Storage overhead |
| Snowflake Clustering | Medium-high | Composite key | Snowflake | Auto-cluster credits |
| Delta Z-order | Medium-high | Multi-column | Spark/Delta | OPTIMIZE compute |
| BigQuery Clustering | Medium-high | Up to 4 cols | BigQuery | Free (slot-based) |
| Bloom Filter Index | Very high | Per-column | Delta/Hudi | Small storage |

### Common Mistakes
- ❌ Partitioning on high-cardinality columns (patient_id) → millions of tiny files → metadata storm
- ❌ Too many partition levels (year/month/day/hour/source) → listing directories becomes the bottleneck
- ❌ Z-ordering on 5+ columns — each additional column adds diminishing returns
- ❌ Using Snowflake auto-clustering on low-cardinality columns (status: 2 values) — wastes credits, no pruning benefit
- ❌ Not running OPTIMIZE after Z-order — Z-order only applies to files written after the OPTIMIZE run
- ✅ Rule of thumb: Partition by date; cluster/Z-order by the remaining high-frequency filter columns

### Interview Questions
1. How does Snowflake micro-partition pruning differ from Hive-style directory partitioning?
2. When would you choose Z-ordering over partitioning?
3. What is the small file problem and how does Delta's OPTIMIZE address it?
4. Why does over-partitioning hurt query performance?
5. How does a bloom filter index complement Z-ordering?

---

## 2.4 Compression

### Theory
Compression reduces storage cost and improves query performance (less I/O = faster reads).

| Codec | Speed | Ratio | Splittable | Best Use |
|---|---|---|---|---|
| Snappy | Very fast | ~2x | No (in Parquet) | Default Parquet/Spark |
| Zstd | Fast | ~3-4x | No (in Parquet) | Better ratio, modern choice |
| Gzip | Slow | ~4x | No | Cold storage, small files |
| LZ4 | Fastest | ~1.5x | No | Ultra-low latency ingestion |
| Brotli | Slow | ~4x | No | Web transfer, HTTP |
| Snappy (raw) | Fast | ~2x | Yes | Splittable raw files |

**Important:** Within Parquet/ORC, "splittable" is handled at row group level — codec splittability matters only for raw files (CSV.gz is not splittable → single mapper processes entire file).

**Column-level encoding (Parquet/ORC):**
- **Dictionary encoding:** Map repeated strings to integer IDs. High cardinality → falls back to plain.
- **RLE (Run-Length Encoding):** `AAABBBCCC` → `3A3B3C`. Excellent for sorted/clustered data.
- **Delta encoding:** Store differences between values. Good for timestamps, sequential IDs.
- **Bit-packing:** Pack small integers into fewer bits.

### Common Mistakes
- ❌ Using gzip for raw Spark input files — not splittable → 1 task reads entire file, no parallelism
- ❌ Snappy everywhere without considering Zstd — Zstd gives 50% better ratio at similar speed
- ❌ Assuming compression always speeds up queries — on CPU-bound workloads, decompression adds overhead
- ✅ For Parquet on S3: Snappy (default, fast reads) or Zstd (better ratio, similar speed)
- ✅ For cold archive: Gzip acceptable (nobody queries it fast anyway)

### Interview Questions
1. Why is gzip a bad choice for Spark input files?
2. What is RLE encoding and when does it provide the best compression?
3. Why does Zstd outperform Snappy for storage-heavy workloads?

---

## 2.5 Small File Problem

### Theory
Small files (< 64MB) cause severe performance degradation in distributed systems:

**Why it happens:**
- Streaming ingestion writes one file per micro-batch per partition
- High-cardinality partitioning creates many tiny partition directories
- Frequent small appends without compaction

**Why it hurts:**
- **HDFS/S3 NameNode overhead:** Each file = one metadata entry. Millions of files = NameNode memory exhaustion (HDFS) or S3 LIST API throttling.
- **Spark task overhead:** Each small file = one task. Millions of tasks → driver scheduling overhead dominates compute time.
- **Query planning cost:** Parquet footer reads for each file add latency before any data is read.

**Solutions:**

| Approach | Tool | How |
|---|---|---|
| Compaction | Delta OPTIMIZE | Merge small files into 128MB-1GB target |
| Coalesce | Spark | `df.coalesce(N).write.parquet(...)` before write |
| Repartition | Spark | `df.repartition(N, col).write.parquet(...)` |
| Auto-compaction | Delta (Databricks) | `delta.autoOptimize.autoCompact=true` |
| Streaming compaction | Hudi | Async compaction service |
| Table maintenance | Iceberg | `CALL system.rewrite_data_files(...)` |

```python
# Spark: control output file count
df.repartition(200)  \          # for large datasets
    .write.parquet("s3://...")

df.coalesce(10) \               # reduce partitions (no shuffle)
    .write.parquet("s3://...")

# Delta: auto-optimize on write
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Iceberg: rewrite small files
spark.sql("""
    CALL catalog.system.rewrite_data_files(
        table => 'db.claims',
        options => map('target-file-size-bytes', '134217728')
    )
""")
```

### Common Mistakes
- ❌ Streaming directly to partitioned Parquet without compaction — creates millions of files in days
- ❌ Using `repartition()` when `coalesce()` is sufficient — repartition causes full shuffle
- ❌ Setting target file size too large (> 1GB) — reduces parallelism on reads
- ✅ Target: 128MB–512MB per file for balanced parallelism and metadata overhead
- ✅ Schedule OPTIMIZE/compaction as a daily maintenance job, not ad-hoc

### Interview Questions
1. Why does the small file problem cause Spark job slowdowns even before data is read?
2. What is the difference between `coalesce()` and `repartition()` in Spark?
3. How does Delta Lake's OPTIMIZE command resolve small files?
4. How does streaming ingestion cause the small file problem and how do you mitigate it?

---

*Next: Chunk 03 — Data Modeling Part 1 (ER Modeling, Dimensional Modeling, Star/Snowflake Schema, Fact Table Types)*
# DE Master Doc — Chunk 03: Data Modeling Part 1
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 3.1 ER Modeling

### Theory

**Three modeling layers in enterprise practice:**

| Layer | Purpose | Audience | Tool |
|---|---|---|---|
| CDM (Conceptual) | Business entities + relationships, no attributes | Business stakeholders | erwin/SqlDBM high-level |
| LDM (Logical) | Full attributes, PKs/FKs, normalization applied | Data Architects | erwin/SqlDBM |
| PDM (Physical) | LDM + DB-specific constraints, indexes, partitioning | DBAs/Engineers | erwin/SqlDBM/DDL |

**Normalization Forms:**

| Form | Rule | Fixes |
|---|---|---|
| 1NF | Atomic values, no repeating groups | Multi-valued cells (phone1, phone2 columns) |
| 2NF | 1NF + no partial dependency on composite PK | Attributes depending on part of composite key |
| 3NF | 2NF + no transitive dependency | Non-key attributes depending on other non-key attributes |
| BCNF | Every determinant is a candidate key | Anomalies 3NF misses with overlapping candidate keys |
| 4NF | BCNF + no multi-valued dependencies | Independent multi-valued facts in one table |

**Cardinality (Crow's Foot notation):**
```
‖──────< = One-to-Many (mandatory both sides)
O──────< = One-to-Many (optional on one side)
>──────< = Many-to-Many (requires bridge/junction table)
‖──────‖ = One-to-One
```

### Production Example — 3NF Violation and Fix
```sql
-- ❌ 3NF violation: payer_region depends on payer_name, not claim_id
CREATE TABLE claims_bad (
    claim_id      INT PRIMARY KEY,
    payer_name    VARCHAR(100),
    payer_region  VARCHAR(50),   -- transitive: claim→payer→region
    amount        DECIMAL(10,2)
);

-- ✅ Fix: decompose — remove transitive dependency
CREATE TABLE payers (
    payer_id      INT PRIMARY KEY,
    payer_name    VARCHAR(100),
    payer_region  VARCHAR(50)
);
CREATE TABLE claims (
    claim_id      INT PRIMARY KEY,
    payer_id      INT REFERENCES payers(payer_id),
    amount        DECIMAL(10,2)
);
```

### ER Diagram — Healthcare CDM
```
       PATIENT ─────────< CLAIM >──────── PAYER
                               │
                          PROCEDURE
                               │
                          PROVIDER
```

### LDM (Logical — 3NF)
```
PATIENT(patient_id PK, name, dob, gender, ssn)
    │1
    │∞
CLAIM(claim_id PK, patient_id FK, payer_id FK,
      provider_id FK, claim_date, amount, status)
    │∞                    │1
PAYER(payer_id PK, name, region, contract_type)
PROVIDER(provider_id PK, name, npi, specialty, state)
CLAIM_PROCEDURE(claim_id FK, procedure_id FK, quantity)  ← bridge
PROCEDURE(procedure_id PK, cpt_code, description, fee)
```

### Tradeoffs
| Decision | Normalized (3NF) | Denormalized |
|---|---|---|
| Write performance | Excellent (minimal redundancy) | Poor (update anomalies) |
| Read performance | Poor (many JOINs) | Excellent (fewer JOINs) |
| Storage | Efficient | Redundant |
| Data integrity | Strong (FK constraints) | Weak (must enforce in app) |
| Best fit | OLTP | OLAP / DWH |

### Common Mistakes
- ❌ Skipping CDM — jumping straight to LDM without business alignment leads to rework
- ❌ Normalizing OLAP models — 3NF in a DWH creates too many JOINs, kills query performance
- ❌ Composite PKs in dimensional models — makes FK references painful; use surrogate keys
- ❌ Missing cardinality — "Customer has Orders" without defining mandatory/optional breaks downstream
- ✅ Always document assumed cardinality with business stakeholders before modeling

### Interview Questions
1. Explain a transitive dependency with a real-world example.
2. When would you intentionally denormalize a 3NF model?
3. What is the difference between CDM, LDM, and PDM in your delivery workflow?
4. How does BCNF differ from 3NF? Give an example where 3NF holds but BCNF is violated.
5. Why are composite primary keys problematic in dimensional modeling?

---

## 3.2 Dimensional Modeling (Kimball)

### Theory

**Kimball's core philosophy:** Build business-process-centric data marts, connected by conformed dimensions. Bottom-up. Optimized for business user query patterns.

**Four-step Dimensional Design Process:**
1. **Select the business process** (claims processing, patient encounters, eligibility)
2. **Declare the grain** (one row = one claim line item / one encounter / one eligibility check)
3. **Identify the dimensions** (patient, provider, payer, date, diagnosis)
4. **Identify the facts** (claim_amount, approved_amount, processing_days, line_count)

**Grain is the most critical decision.** Get it wrong → model is useless or forces redesign.

**Fact Table:** Central table. Numeric measurable metrics + FK references to dimensions. Append-only (mostly). Narrow and tall (many rows, few columns beyond FKs).

**Dimension Table:** Descriptive context — Who, What, Where, When, Why. Wide and short (few rows, many columns). Denormalized for query convenience. Surrogate key as PK, natural key preserved as attribute.

**Enterprise Bus Matrix:** Grid mapping business processes (rows) × conformed dimensions (columns). Tick marks show which dimensions apply to which fact tables. Critical for planning cross-process drill-across queries.

### Bus Matrix Example — Healthcare
| Business Process | dim_date | dim_patient | dim_provider | dim_payer | dim_diagnosis | dim_procedure |
|---|---|---|---|---|---|---|
| Claims Processing | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Eligibility Check | ✓ | ✓ | — | ✓ | — | — |
| Patient Encounter | ✓ | ✓ | ✓ | — | ✓ | ✓ |
| Prior Auth | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

**Conformed dimensions** (dim_patient, dim_date) enable drill-across: "Show claim spend for patients with > 5 encounters" — joins two fact tables via shared conformed dims.

### Star vs Snowflake Schema

**Star Schema:** Fact table directly joined to fully denormalized dimension tables. One join per dimension.
```
                    dim_date
                       │
dim_payer ──── fact_claims ──── dim_patient
                       │
                  dim_provider
                       │
                  dim_diagnosis
```

**Snowflake Schema:** Dimension tables normalized into sub-dimensions. Saves storage, adds JOINs.
```
dim_payer ──── fact_claims ──── dim_patient ──── dim_geography
                                                       │
                                                  dim_region
```

### Tradeoffs — Star vs Snowflake
| Dimension | Star Schema | Snowflake Schema |
|---|---|---|
| Query performance | Better (fewer JOINs) | More JOINs |
| Storage | More redundancy | Less redundancy |
| Maintenance | Simpler | Complex |
| BI tool friendly | Excellent (Power BI, Tableau native) | Moderate |
| Cloud DWH verdict | **Preferred** | Legacy / specific use |
| ETL complexity | Simpler | More complex |

**2024 verdict:** Storage is cheap. Star schema wins almost always. Snowflake schema only justified for very large dimensions with many slowly-changing attributes shared across multiple fact tables.

### Production DDL — Snowflake Star Schema
```sql
-- Dimension: Patient
CREATE TABLE dim_patient (
    patient_sk        INT AUTOINCREMENT PRIMARY KEY,  -- surrogate
    patient_nk        VARCHAR(50)    NOT NULL,         -- natural key
    patient_name      VARCHAR(200),
    date_of_birth     DATE,
    gender_code       CHAR(1),
    address_state     CHAR(2),
    insurance_plan    VARCHAR(100),
    eff_start_date    DATE           NOT NULL,
    eff_end_date      DATE,
    is_current        BOOLEAN        DEFAULT TRUE
);

-- Dimension: Date (pre-populated calendar)
CREATE TABLE dim_date (
    date_sk           INT PRIMARY KEY,  -- YYYYMMDD integer
    full_date         DATE,
    year              INT,
    quarter           INT,
    month             INT,
    month_name        VARCHAR(20),
    week_of_year      INT,
    day_of_week       INT,
    is_weekend        BOOLEAN,
    is_holiday        BOOLEAN,
    fiscal_year       INT,
    fiscal_quarter    INT
);

-- Fact: Claims
CREATE TABLE fact_claims (
    claim_sk          INT AUTOINCREMENT PRIMARY KEY,
    claim_nk          VARCHAR(50),           -- degenerate dimension
    patient_sk        INT REFERENCES dim_patient(patient_sk),
    provider_sk       INT REFERENCES dim_provider(provider_sk),
    payer_sk          INT REFERENCES dim_payer(payer_sk),
    service_date_sk   INT REFERENCES dim_date(date_sk),
    submitted_date_sk INT REFERENCES dim_date(date_sk),
    diagnosis_sk      INT REFERENCES dim_diagnosis(diagnosis_sk),
    -- Facts (measures)
    billed_amount     DECIMAL(12,2),
    allowed_amount    DECIMAL(12,2),
    paid_amount       DECIMAL(12,2),
    patient_liability DECIMAL(12,2),
    line_item_count   INT,
    processing_days   INT
);
```

### Common Mistakes
- ❌ Declaring grain at the wrong level — grain = "one claim" but data has multiple line items → aggregation errors
- ❌ Putting text descriptions in fact tables — descriptions belong in dimensions
- ❌ Missing dim_date — date stored as VARCHAR in fact → can't use date math, can't filter efficiently
- ❌ Too many facts in one fact table mixing different grains — creates division of measures errors
- ✅ Always populate dim_date independently (pre-built calendar, not generated from facts)
- ✅ One fact table = one grain. Different grains = different fact tables.

### Interview Questions
1. What is grain and why must it be declared before designing a fact table?
2. What is a conformed dimension and why is it architecturally critical?
3. When would you choose snowflake schema over star schema in 2024?
4. What is a drill-across query and how does the bus matrix enable it?
5. Why is dim_date pre-populated as a separate table rather than derived from fact dates?

---

## 3.3 Fact Table Types

### Theory

**Four fact table types — each serves a different analytical pattern:**

---

**1. Transaction Fact Table**
- One row per business event/transaction.
- Append-only. Never updated.
- Finest grain. Largest table.
- Examples: individual claim, POS transaction, trade execution, log event.
- Best for: event-level analysis, trend analysis, funnel analysis.

```sql
CREATE TABLE fact_claim_transactions (
    claim_sk          INT,
    patient_sk        INT,
    payer_sk          INT,
    service_date_sk   INT,
    claim_nk          VARCHAR(50),  -- degenerate dimension
    billed_amount     DECIMAL(12,2),
    paid_amount       DECIMAL(12,2),
    line_item_count   INT
    -- Append only. No updates.
);
```

---

**2. Periodic Snapshot Fact Table**
- One row per entity per time period (daily, weekly, monthly).
- Captures cumulative state at regular intervals.
- Efficiently answers "what was the state on date X?" without scanning all transactions.
- Examples: account balance at month-end, inventory on hand daily, headcount weekly, premium balance.

```sql
CREATE TABLE fact_monthly_claim_summary (
    summary_month_sk  INT,          -- FK to dim_date (month grain)
    payer_sk          INT,
    total_claims      INT,
    total_billed      DECIMAL(12,2),
    total_paid        DECIMAL(12,2),
    denial_rate       DECIMAL(5,4),
    avg_processing_days DECIMAL(5,1)
    -- One row per payer per month. Inserted monthly.
);
```

---

**3. Accumulating Snapshot Fact Table**
- One row per process instance tracking its entire lifecycle.
- Rows are **UPDATED** as milestones are reached (only fact type that gets updated).
- Contains multiple date FKs — one per milestone.
- Examples: claim lifecycle, order fulfillment, loan application, insurance policy lifecycle.
- Best for: pipeline/funnel analysis, SLA tracking, elapsed time between milestones.

```sql
CREATE TABLE fact_claim_lifecycle (
    claim_sk              INT PRIMARY KEY,
    patient_sk            INT,
    payer_sk              INT,
    -- Milestone date FKs (NULL until milestone reached)
    submitted_date_sk     INT,
    reviewed_date_sk      INT,
    approved_date_sk      INT,
    paid_date_sk          INT,
    denied_date_sk        INT,
    -- Lag measures (computed on each update)
    days_submit_to_review INT,
    days_review_to_decision INT,
    days_to_payment       INT,
    -- Measures
    claim_amount          DECIMAL(12,2),
    approved_amount       DECIMAL(12,2),
    current_status        VARCHAR(20)
    -- Row UPDATED each time a milestone date is populated
);
```

---

**4. Factless Fact Table**
- No numeric measures. Records occurrence of an event or coverage relationship.
- Two sub-types:
  - **Event factless:** Did event X occur? (student attendance, patient visit)
  - **Coverage factless:** What entities are covered by Y? (product promotions, eligibility)

```sql
-- Event factless: patient appointment attendance
CREATE TABLE fact_appointment_attendance (
    appointment_date_sk INT,
    patient_sk          INT,
    provider_sk         INT,
    location_sk         INT,
    attended_flag       CHAR(1)  -- Y/N (or just presence = attended)
    -- No numeric measure. COUNT(*) IS the measure.
);

-- Coverage factless: which products are on promotion
CREATE TABLE fact_promotion_coverage (
    promotion_date_sk   INT,
    product_sk          INT,
    store_sk            INT,
    promotion_sk        INT
    -- Query: "Which products had no promotion this month?"
    -- LEFT JOIN to this table, find NULLs
);
```

### Tradeoffs
| Type | Updated? | Grain | Best For |
|---|---|---|---|
| Transaction | Never | One event | Event analysis, trend |
| Periodic Snapshot | Never (insert only) | Entity × time period | Point-in-time state |
| Accumulating Snapshot | Yes (milestones) | One process instance | Pipeline/SLA tracking |
| Factless | Never | Event/coverage occurrence | Coverage, attendance |

### Common Mistakes
- ❌ Using transaction fact for "current balance" queries — must scan all history; use periodic snapshot
- ❌ Forgetting that accumulating snapshot rows get updated — ETL must do MERGE/UPSERT not INSERT
- ❌ Mixing grains in one fact table — e.g., claim header + claim line items in same table → wrong aggregations
- ❌ Skipping factless fact tables — trying to solve coverage questions in dimension tables instead
- ✅ Lag measures in accumulating snapshot should be computed at ETL time, stored as integers (days)

### Interview Questions
1. Why is an accumulating snapshot the only fact type that gets updated?
2. How would you model "monthly account balance" — transaction fact or periodic snapshot?
3. Give an example where a factless fact table is the correct solution.
4. What is a lag measure and how is it computed in an accumulating snapshot?
5. What goes wrong if you mix two different grains in one fact table?

---

## 3.4 Special Dimension Types

### Theory

**Degenerate Dimension (DD)**
- Dimension attribute stored directly in fact table — no separate dim table.
- Has no additional attributes beyond the identifier itself.
- Examples: invoice_number, order_number, claim_number, transaction_id.
- Used for grouping and filtering (GROUP BY claim_nk, WHERE claim_nk = 'X').
- No surrogate key needed — the natural key IS the degenerate dimension.

**Junk Dimension**
- Combines multiple low-cardinality boolean/flag attributes into one dimension.
- Avoids cluttering fact table with many tiny FKs.
- Pre-populated with all possible flag combinations.
- Example: dim_claim_flags combining is_urgent + is_resubmission + is_appeal + has_attachment (2⁴ = 16 rows max).

```sql
CREATE TABLE dim_claim_flags (
    claim_flags_sk    INT PRIMARY KEY,
    is_urgent         BOOLEAN,
    is_resubmission   BOOLEAN,
    is_appeal         BOOLEAN,
    has_attachment    BOOLEAN
);
-- Pre-load all 16 combinations (2^4)
-- fact_claims → claim_flags_sk FK (1 FK replaces 4 columns)
```

**Conformed Dimension**
- Shared dimension used identically across multiple fact tables and subject areas.
- Examples: dim_date, dim_patient, dim_product.
- Enables cross-process drill-across queries.
- Requires enterprise governance — definition changes must propagate to all marts.
- Owned by central data team or governed via data product (Data Mesh).

**Role-Playing Dimension**
- Single physical dimension table aliased multiple times in same fact table.
- Example: dim_date plays roles as service_date, submitted_date, paid_date in fact_claims.
- Only one physical table — multiple logical views/aliases.

```sql
-- Role-playing dim_date in fact_claims
SELECT
    f.claim_amount,
    svc.full_date   AS service_date,
    sub.full_date   AS submitted_date,
    paid.full_date  AS paid_date
FROM fact_claims f
JOIN dim_date svc  ON f.service_date_sk  = svc.date_sk
JOIN dim_date sub  ON f.submitted_date_sk = sub.date_sk
JOIN dim_date paid ON f.paid_date_sk     = paid.date_sk;
```

**Outrigger Dimension**
- Secondary dimension table hanging off a primary dimension (not off the fact table).
- Example: dim_geography hangs off dim_provider (provider → geography, not claim → geography directly).
- Used sparingly — introduces a second-level join, complicates BI tool navigation.

**Mini-Dimension (SCD Type 5)**
- Frequently changing attributes pulled out of a large dimension into a separate small dimension.
- Avoids row explosion in SCD Type 2 dim from high-velocity attribute changes.
- Example: Customer's income_band, credit_score_tier, risk_category pulled into dim_customer_profile (mini-dim).
- Fact table carries FK to both main dim_customer and dim_customer_profile.

### Quick Reference
| Dimension Type | Purpose | Stored In |
|---|---|---|
| Degenerate | Identifier with no attributes | Fact table directly |
| Junk | Grouped low-cardinality flags | Separate dim, all combinations |
| Conformed | Shared across fact tables | Shared dim table |
| Role-Playing | Same dim, different contexts | One physical table, multiple aliases |
| Outrigger | Secondary context for a dim | Hangs off primary dim |
| Mini-Dimension | High-velocity volatile attributes | Separate small dim |

### Common Mistakes
- ❌ Creating separate dim tables for degenerate dimensions — wastes joins for no benefit
- ❌ Junk dimension with too many columns — 10 boolean flags = 1024 rows, manageable; 20 flags = 1M rows, not a junk dim anymore
- ❌ Conformed dimensions maintained by multiple teams independently — definitions drift, drill-across breaks
- ❌ Over-using outrigger dims — BI tools struggle with 3-level joins
- ✅ Role-playing dims: always create named views for each role so BI tools see semantic names

### Interview Questions
1. When should you use a junk dimension vs keeping flags in the fact table directly?
2. What makes a dimension "conformed" and why is governance critical?
3. Give a role-playing dimension example in a healthcare or BFSI context.
4. Why is a degenerate dimension stored in the fact table instead of a separate dim?
5. What is a mini-dimension and what problem does it solve that SCD Type 2 cannot?

---

*Next: Chunk 04 — Data Modeling Part 2 (SCD Types 0–7, Anchor Modeling, Surrogate vs Natural Keys)*
# DE Master Doc — Chunk 04: Data Modeling Part 2
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 4.1 SCD Types 0–7 — Complete Reference

### Theory

Slowly Changing Dimensions (SCDs) define how dimension attribute changes are handled over time.

---

**Type 0 — Retain Original**
- No change ever. Attribute keeps its original value regardless of source updates.
- Use: fixed attributes — original contract date, birth date, original hire date.
- Implementation: no ETL logic needed; simply ignore incoming changes.

---

**Type 1 — Overwrite**
- Overwrite existing value with new value. No history kept.
- Use: correcting errors (wrong postal code, typo in name), non-analytical attributes.
- Risk: breaks historical analysis of that attribute — historical facts now reflect current value.

```sql
-- Type 1: simple UPDATE
UPDATE dim_patient
SET    address_state = 'CA'
WHERE  patient_nk = 'PAT-123'
AND    is_current = TRUE;
```

---

**Type 2 — Add New Row (most common)**
- Insert new row with new surrogate key on every change.
- Old row expired (end_date populated, is_current = FALSE).
- Full history preserved. Fact rows point to surrogate key = automatically capture historical state.
- Use: address changes, territory reassignment, insurance plan changes, name changes.

```sql
-- Type 2: expire old row, insert new row
UPDATE dim_patient
SET    eff_end_date = CURRENT_DATE - 1,
       is_current   = FALSE
WHERE  patient_nk  = 'PAT-123'
AND    is_current   = TRUE;

INSERT INTO dim_patient
    (patient_nk, patient_name, address_state, insurance_plan,
     eff_start_date, eff_end_date, is_current)
VALUES
    ('PAT-123', 'Jane Doe', 'CA', 'BlueCross PPO',
     CURRENT_DATE, NULL, TRUE);
```

**dbt snapshot (Type 2 automated):**
```yaml
# snapshots/dim_patient_snapshot.sql
{% snapshot dim_patient_snapshot %}
  {{ config(
      target_schema = 'snapshots',
      unique_key    = 'patient_nk',
      strategy      = 'check',
      check_cols    = ['address_state', 'insurance_plan', 'patient_name']
  ) }}
  SELECT * FROM {{ source('ods', 'patients') }}
{% endsnapshot %}
```

---

**Type 3 — Add New Column**
- Add `previous_value` column alongside `current_value`.
- Remembers only one prior value — breaks on 3+ changes.
- Use: limited lookback scenarios (current vs prior sales territory for one reorg cycle).

```sql
ALTER TABLE dim_sales_rep
ADD COLUMN prior_territory VARCHAR(100);

UPDATE dim_sales_rep
SET    prior_territory   = current_territory,
       current_territory = 'West Region'
WHERE  rep_nk = 'REP-007';
```

---

**Type 4 — History Table**
- Split: current dim table (always current values) + separate history table (all versions).
- Current dim stays lean → fast current-state lookups.
- History table used only when historical context needed.

```sql
-- Current table: always latest
CREATE TABLE dim_patient_current (patient_nk PK, name, state, plan, ...);

-- History table: all versions
CREATE TABLE dim_patient_history (
    patient_nk    VARCHAR(50),
    name          VARCHAR(200),
    state         CHAR(2),
    plan          VARCHAR(100),
    valid_from    DATE,
    valid_to      DATE
);
```

---

**Type 5 — Mini-Dimension + Type 1 Outrigger**
- Frequently changing attributes separated into mini-dimension (Type 5).
- Avoids SCD Type 2 row explosion for high-velocity volatile attributes.
- Fact table carries FK to both main dim and mini-dim.
- Use: customer risk tier, credit score band, income bracket — changes monthly.

```sql
-- Main dim: stable attributes (Type 2)
dim_customer(customer_sk PK, customer_nk, name, dob, eff_start, eff_end)

-- Mini-dim: volatile attributes (Type 1 — just current)
dim_customer_profile(profile_sk PK, income_band, credit_tier, risk_category)

-- Fact: FK to both
fact_transactions(txn_sk, customer_sk, profile_sk, amount, ...)
```

---

**Type 6 — Hybrid (Type 1 + 2 + 3)**
- New row on every change (Type 2 behavior).
- PLUS: `current_value` column updated on ALL rows for the same entity (Type 1 overlay).
- PLUS: `prior_value` column retains previous value (Type 3 element).
- Enables both "what was true then" (via surrogate key join) AND "what is true now" (via current_value column).

```sql
CREATE TABLE dim_patient_t6 (
    patient_sk         INT PRIMARY KEY,          -- Type 2 SK
    patient_nk         VARCHAR(50),
    address_state      CHAR(2),                  -- historical value (Type 2)
    current_state      CHAR(2),                  -- always current (Type 1 overlay)
    prior_state        CHAR(2),                  -- previous value (Type 3)
    eff_start_date     DATE,
    eff_end_date       DATE,
    is_current         BOOLEAN
);
```

---

**Type 7 — Dual Type 1 + Type 2**
- Fact table carries two FKs to the same dimension:
  - FK to current dim row (always points to latest surrogate key) → current-state analysis
  - FK to historical dim row (point-in-time surrogate key) → historical analysis
- Advanced, rarely implemented in practice.

---

### SCD Type Decision Guide
| Situation | SCD Type |
|---|---|
| Attribute never changes (birth date) | Type 0 |
| Error correction, history not needed | Type 1 |
| Full history required (standard) | Type 2 |
| One lookback period only | Type 3 |
| Fast current lookups + full history | Type 4 |
| High-velocity volatile attributes | Type 5 (mini-dim) |
| Both historical + current on same row | Type 6 |
| Dual current + historical FK in fact | Type 7 |

### Tradeoffs
| Type | Storage | History | Complexity | Query Performance |
|---|---|---|---|---|
| 0 | Minimal | None | Minimal | Fast |
| 1 | Minimal | None | Low | Fast |
| 2 | High (row explosion) | Full | Medium | Good (with SK) |
| 3 | Low | One version | Low | Fast |
| 4 | Medium | Full (history table) | Medium | Fast (current) |
| 5 | Medium | Full (main) | High | Good |
| 6 | High | Full | High | Good |
| 7 | High | Full | Very High | Good |

### Common Mistakes
- ❌ Using Type 1 on attributes used in historical trend analysis — destroys analytical integrity
- ❌ SCD Type 2 without is_current flag — must always filter `WHERE is_current = TRUE` for current state or use eff_end_date IS NULL
- ❌ Forgetting to update fact row to new surrogate key — old facts still point to expired row
- ❌ Using SCD Type 2 for high-velocity attributes (daily price changes) — billions of rows quickly
- ❌ No hash_diff on satellite/dim → full attribute comparison on every load → slow ETL
- ✅ Always store hash_diff (MD5 of all tracked attributes) — detect changes in one comparison
- ✅ For Snowflake: use MERGE statement for SCD Type 2 ETL — atomic upsert

### Interview Questions
1. Why does a surrogate key enable correct historical join in SCD Type 2?
2. What is the performance trade-off of SCD Type 2 on large dimensions?
3. When would SCD Type 1 be preferred despite losing history?
4. Explain SCD Type 6 and give a use case where it outperforms pure Type 2.
5. How do you handle late-arriving SCD Type 2 records?
6. What is a hash_diff column and what ETL problem does it solve?

---

## 4.2 Data Vault 2.0 — Deep Dive

### Theory

**Core design philosophy:** Separate structure (business keys, relationships) from context (attributes). Insert-only. Auditable. Parallel-loadable. Source-agnostic.

---

**Hub**
- Contains the business key — the unique identifier meaningful to the business.
- One row per distinct business key ever seen.
- Insert-only. Never updated.
- Metadata: hash_key, load_date, record_source, business_key.

```sql
CREATE TABLE HUB_PATIENT (
    PATIENT_HK      CHAR(32)      NOT NULL PRIMARY KEY,  -- MD5(patient_nk)
    LOAD_DATE       TIMESTAMP_NTZ NOT NULL,
    RECORD_SOURCE   VARCHAR(100)  NOT NULL,
    PATIENT_BK      VARCHAR(50)   NOT NULL               -- business key
);
```

**Link**
- Represents a relationship between two or more hubs.
- Captures the business event/transaction at its intersection.
- Insert-only. Hash_key = MD5(all FK hash_keys concatenated).
- Many-to-many natively. No need for bridge tables.

```sql
CREATE TABLE LNK_CLAIM_PATIENT (
    CLAIM_PATIENT_HK  CHAR(32)      NOT NULL PRIMARY KEY,
    LOAD_DATE         TIMESTAMP_NTZ NOT NULL,
    RECORD_SOURCE     VARCHAR(100)  NOT NULL,
    CLAIM_HK          CHAR(32)      NOT NULL,
    PATIENT_HK        CHAR(32)      NOT NULL
);
```

**Satellite**
- Stores descriptive attributes for a hub or link.
- Multiple satellites per hub: one per source system, one per rate-of-change grouping.
- Insert-only with end-dating pattern. Hash_diff detects attribute changes.

```sql
CREATE TABLE SAT_PATIENT_DEMOGRAPHICS (
    PATIENT_HK      CHAR(32)      NOT NULL,
    LOAD_DATE       TIMESTAMP_NTZ NOT NULL,
    LOAD_END_DATE   TIMESTAMP_NTZ,            -- NULL = current row
    RECORD_SOURCE   VARCHAR(100)  NOT NULL,
    HASH_DIFF       CHAR(32),                 -- MD5(all tracked attrs)
    PATIENT_NAME    VARCHAR(200),
    DATE_OF_BIRTH   DATE,
    GENDER_CODE     CHAR(1),
    PRIMARY KEY (PATIENT_HK, LOAD_DATE)
);
```

---

**Raw Vault (RDV)**
- Direct load from source. Zero transformations. No business rules.
- Source-aligned. Preserves all data as-is. Complete audit trail.
- Load in parallel: HUB load and SAT load are independent (hash_key is deterministic).

**Business Vault (BV)**
- Derived vault objects with business rules applied.
- Computed Satellites (SAT_COMP_*): derived/calculated attributes.
- Point-in-Time (PIT) tables: performance optimization — pre-join hub + satellite load_dates.
- Bridge Tables: pre-join Links + Hubs for complex many-to-many navigation.

**PIT Table:**
```sql
-- PIT: one row per entity per snapshot date with load_dates from each satellite
CREATE TABLE PIT_PATIENT (
    PATIENT_HK          CHAR(32)      NOT NULL,
    SNAPSHOT_DATE       DATE          NOT NULL,
    -- Load dates from each satellite (NULL if no record on that date)
    SAT_DEMOGRAPHICS_LDT  TIMESTAMP_NTZ,
    SAT_CONTACT_LDT       TIMESTAMP_NTZ,
    SAT_INSURANCE_LDT     TIMESTAMP_NTZ,
    PRIMARY KEY (PATIENT_HK, SNAPSHOT_DATE)
);
-- Eliminates expensive satellite date-range joins at query time
-- Query: JOIN hub → PIT → satellite (1 join per sat, no date range scan)
```

**Information Mart (delivery layer)**
- Kimball star schemas built ON TOP of Business Vault.
- Business rules fully applied. Optimized for BI tools.
- DV2 provides auditability; Info Mart provides performance.

### Architecture
```
Source Systems (EHR, Claims, CRM)
         │
         ▼
   Staging Area (raw extract, no transform)
         │
         ▼
   Raw Vault (RDV)
   ┌─────────────────────────────────────┐
   │  HUB_PATIENT  HUB_CLAIM  HUB_PAYER │
   │  LNK_CLAIM_PATIENT                  │
   │  SAT_PATIENT_*  SAT_CLAIM_*         │
   └─────────────────────────────────────┘
         │  (business rules applied)
         ▼
   Business Vault (BV)
   ┌─────────────────────────────────────┐
   │  SAT_COMP_* (computed)             │
   │  PIT_PATIENT  BRIDGE_*             │
   └─────────────────────────────────────┘
         │
         ▼
   Information Marts (star schemas)
   ┌─────────────────────────────────────┐
   │  fact_claims  dim_patient  dim_date │
   └─────────────────────────────────────┘
         │
         ▼
   BI Tools / Consumers
```

### DV2 vs Kimball
| Dimension | Data Vault 2.0 | Kimball Dimensional |
|---|---|---|
| Agility (add new source) | High — add satellite, no rebuild | Low — schema changes cascade |
| Auditability | Full (insert-only, all history) | Partial (depends on SCD type) |
| Query performance | Complex joins (PIT helps) | Fast (star schema) |
| Business user access | Poor (needs Info Mart on top) | Direct BI access |
| Multi-source integration | Excellent (source isolation in sats) | Complex ETL required |
| Parallel loading | Fully parallel | Sequential dependencies |
| Regulatory/audit fit | Excellent | Moderate |
| Best fit | Enterprise DWH, multi-source, regulatory | Departmental BI, performance-critical |

**Best practice:** DV2 in the integration layer (Raw + Business Vault) → Kimball star schemas as Information Marts. Best of both worlds.

### Common Mistakes
- ❌ Putting business rules in Raw Vault — RDV must be source-faithful
- ❌ Using sequence-generated surrogate keys in DV2 — breaks parallel loading (sequences require coordination)
- ❌ One satellite per hub with all attributes — mix high-frequency + low-frequency attrs = excessive row proliferation
- ❌ Skipping PIT tables and querying satellites with date range joins — kills performance
- ❌ Hash key collision risk ignored — MD5 has theoretical collision risk; SHA-256 for high-stakes systems
- ✅ Split satellites by rate-of-change: SAT_PATIENT_DEMOGRAPHICS (rarely changes) + SAT_PATIENT_STATUS (changes frequently)
- ✅ Hash_diff on every satellite — detect attribute changes in O(1) comparison

### Interview Questions
1. Why does Data Vault use a hash key instead of a sequence-generated surrogate?
2. What is a PIT table and how does it accelerate satellite queries?
3. How does DV2 handle multi-source integration differently from Kimball?
4. Why is DV2 preferred for regulatory/audit requirements?
5. What is a hash_diff in a satellite and what ETL problem does it solve?
6. How do you split satellites for a hub with many attributes?

---

## 4.3 Anchor Modeling

### Theory

**Anchor Modeling** is a sixth normal form (6NF) approach to data modeling. Taken to its extreme: every attribute gets its own table.

**Core components:**
- **Anchor:** Equivalent to Hub. Surrogate key only. One table per entity.
- **Attribute:** One table per attribute of the anchor. Each row = one value at one point in time.
- **Tie:** Equivalent to Link. Relationship between two anchors.
- **Knot:** Shared lookup table for enumerated values (reused across entities).

```sql
-- Anchor: just the key
CREATE TABLE Patient (Patient_ID INT PRIMARY KEY);

-- Attribute: one table per attribute
CREATE TABLE Patient_Name (
    Patient_ID  INT,
    Name        VARCHAR(200),
    ValidFrom   DATE,
    PRIMARY KEY (Patient_ID, ValidFrom)
);

CREATE TABLE Patient_State (
    Patient_ID  INT,
    State       CHAR(2),
    ValidFrom   DATE,
    PRIMARY KEY (Patient_ID, ValidFrom)
);
```

### Anchor vs Data Vault 2.0
| Dimension | Anchor Modeling | Data Vault 2.0 |
|---|---|---|
| Normalization | 6NF (one table per attribute) | Flexible (group attrs in satellites) |
| Schema changes | Zero impact (add new attribute table) | Add new satellite |
| Query complexity | Very high (massive JOINs) | High (but PIT helps) |
| Performance | Poor without heavy optimization | Better (PIT, Bridge) |
| Industry adoption | Niche (academic, extreme agility) | Mainstream enterprise |
| Tooling support | Limited | Strong (WhereScape, DBT-vault) |

**Honest assessment:** Anchor Modeling is theoretically elegant but practically painful at scale. Data Vault 2.0 is the pragmatic enterprise choice. Know Anchor Modeling for interviews — rarely implement it.

### Interview Questions
1. What problem does Anchor Modeling solve that Data Vault 2.0 doesn't?
2. Why is Anchor Modeling rarely used in production despite its theoretical advantages?
3. Compare the schema evolution story of Anchor Modeling vs Data Vault 2.0.

---

## 4.4 Surrogate vs Natural Keys

### Theory

**Natural Key (NK / Business Key):**
- Meaningful to the business. Exists in source systems.
- Examples: SSN, employee_id, claim_number, ISIN, NPI.
- Pros: human-readable, no extra join to decode, source-verifiable.
- Cons: can change, can be reused, can be NULL, can be composite, exposes PII, ties you to source semantics.

**Surrogate Key (SK):**
- System-generated, no business meaning. Immutable.
- Pros: stable (immune to NK changes), compact (INT), enables SCD Type 2, protects PII.
- Cons: extra join required to decode to business key.

**Four surrogate key strategies:**

| Type | Generation | Pro | Con | Best Use |
|---|---|---|---|---|
| INT Sequence | DB AUTOINCREMENT | Best join performance | Requires coordination (SPOF in distributed) | DWH star schema dims |
| BIGINT Sequence | Spark monotonically_increasing_id | Distributed-safe | Not globally unique across runs | Spark batch |
| UUID (v4) | Random 128-bit | Globally unique, no coordination | Index fragmentation, 16 bytes | Microservices, APIs |
| Hash Key (MD5/SHA) | Deterministic: hash(NK) | Deterministic, parallel-safe, idempotent | Fixed size, collision risk (MD5) | Data Vault 2.0 |

**Why hash keys win in DV2:**
- Same business key → always same hash → satellites and links can be loaded in parallel without looking up a sequence table.
- Idempotent: re-running the load produces identical hash keys → no duplicates.
- Eliminates sequence table bottleneck in distributed pipelines.

**Why UUID hurts databases:**
- UUID v4 is random → new inserts go to random B-tree positions → index fragmentation.
- Solution: UUID v7 (time-ordered) or ULID — monotonically increasing, globally unique, no fragmentation.

### Key Design Rules
```
Rule 1: Always preserve natural key as attribute alongside surrogate key.
        → You need NK for source reconciliation, debugging, audit.

Rule 2: Never expose surrogate keys to business users or APIs.
        → SKs are internal pipeline plumbing.

Rule 3: In DV2 → hash key. In star schema → INT sequence.
        → Match the key strategy to the architecture.

Rule 4: In Spark → avoid DB sequences. Use hash or monotonically_increasing_id.
        → DB sequence = single-point bottleneck in distributed pipeline.
```

### Production Example
```python
# Spark: hash key generation (DV2 compatible)
from pyspark.sql.functions import md5, concat_ws, upper, trim

df_hub = df_source \
    .withColumn("PATIENT_HK",
        md5(upper(trim(col("PATIENT_BK"))))) \
    .withColumn("LOAD_DATE", current_timestamp()) \
    .withColumn("RECORD_SOURCE", lit("EHR_SYSTEM")) \
    .select("PATIENT_HK", "LOAD_DATE", "RECORD_SOURCE", "PATIENT_BK")

# Snowflake: sequence-based SK for star schema
CREATE SEQUENCE dim_patient_seq START = 1 INCREMENT = 1;

INSERT INTO dim_patient (patient_sk, patient_nk, ...)
SELECT dim_patient_seq.NEXTVAL, patient_nk, ...
FROM staging_patients;
```

### Tradeoffs
| Key Type | Join Perf | Distributed | Deterministic | Size | Best Use |
|---|---|---|---|---|---|
| INT Sequence | Best | Poor | No | 4 bytes | DWH star schema |
| BIGINT Sequence | Good | Partial | No | 8 bytes | Large DWH |
| UUID v4 | Poor (fragmentation) | Best | No | 16 bytes | Microservices |
| UUID v7 / ULID | Good | Best | No | 16 bytes | Modern microservices |
| MD5 Hash | Good | Yes | Yes | 16 bytes | Data Vault 2.0 |
| SHA-256 Hash | Good | Yes | Yes | 32 bytes | High-security DV2 |
| Natural Key | Varies | Yes | Yes | Varies | Staging only |

### Common Mistakes
- ❌ Using UUID as PK in relational DWH — random insert order destroys B-tree index performance
- ❌ Dropping natural key after generating surrogate — loses ability to reconcile with source
- ❌ Using DB AUTOINCREMENT sequence in distributed Spark pipeline — sequence table = single bottleneck
- ❌ MD5 hash without padding/trimming/uppercasing source key first — same NK produces different hashes
- ✅ Normalize NK before hashing: `MD5(UPPER(TRIM(patient_nk)))` — consistent, reproducible
- ✅ Always index natural key column in dim tables for source lookup during ETL

### Interview Questions
1. Why is a hash key preferred over a sequence in Data Vault 2.0?
2. What is UUID index fragmentation and how does UUID v7/ULID solve it?
3. Why must you always preserve the natural key alongside the surrogate key?
4. How do you generate surrogate keys in a distributed Spark pipeline without a sequence table?
5. What is the risk of MD5 collision and when should you use SHA-256 instead?

---

*Next: Chunk 05 — Modern Architectures (Medallion, Lambda/Kappa, Data Mesh, Data Fabric, Lakehouse, Multi-cloud)*
# DE Master Doc — Chunk 05: Modern Data Architectures
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 5.1 Medallion Architecture (Bronze / Silver / Gold)

### Theory

A logical data organization pattern dividing a Lakehouse into progressive quality layers. Each layer adds trust, structure, and business value.

**Bronze Layer (Raw)**
- Exact copy of source data. Zero transformations.
- Append-only. Schema-on-read or light schema enforcement.
- Complete audit trail — every record ever received, including duplicates and bad data.
- Partitioned by ingestion date (not business date).
- Format: Delta Lake / Iceberg with `mergeSchema = true` (tolerate drift).
- Retention: indefinite (or by policy — regulatory minimum 7 years for HIPAA).

**Silver Layer (Cleansed / Conformed)**
- Data quality rules applied: nulls handled, types cast, duplicates removed.
- Cross-source joins: patient from EHR + patient from claims → unified silver patient entity.
- Light business rules (standardize state codes, format phone numbers).
- NOT business-domain specific — reusable across all use cases.
- Schema enforced. Idempotent rewrites safe.
- Format: Delta Lake with schema enforcement + check constraints.

**Gold Layer (Business / Curated)**
- Business-domain specific. Optimized for specific consumers.
- Dimensional models (star schemas) for BI.
- Aggregated KPIs for dashboards.
- ML feature tables for data scientists.
- Regulatory report tables for compliance.
- Multiple gold tables per business domain.
- Format: Delta Lake, optionally exposed via Snowflake external tables or views.

### Architecture Diagram
```
Sources: EHR | Claims | Lab | CRM | Billing | IoT
          │       │      │     │      │       │
          └───────┴──────┴─────┴──────┴───────┘
                          │ (ingest: Kafka / IICS / ADF)
                          ▼
┌──────────────────────────────────────────────────────┐
│  BRONZE — Raw / Append-only                          │
│  s3://datalake/bronze/claims/year=2024/month=01/     │
│  • Exact copy from source                            │
│  • mergeSchema=true (tolerate drift)                 │
│  • Partition: ingestion_year / month / day           │
│  • Retention: forever                                │
└──────────────────────────────────────────────────────┘
                          │ dbt / Spark ETL (DQ rules)
                          ▼
┌──────────────────────────────────────────────────────┐
│  SILVER — Cleansed / Conformed                       │
│  • DQ: nulls, types, dedup, standardization         │
│  • Cross-source entity resolution                    │
│  • Schema enforced (not tolerant)                    │
│  • Reusable across all gold consumers               │
└──────────────────────────────────────────────────────┘
                          │ dbt models / aggregations
                          ▼
┌──────────────────────────────────────────────────────┐
│  GOLD — Business                                     │
│  ┌────────────┐ ┌───────────────┐ ┌───────────────┐ │
│  │ Star Schema│ │ ML Features   │ │ Regulatory    │ │
│  │ (BI/Power BI)│ (Fraud Model) │ │ (HIPAA/SOX)  │ │
│  └────────────┘ └───────────────┘ └───────────────┘ │
└──────────────────────────────────────────────────────┘
```

### Production Example (BFSI — Claims Pipeline)
```python
# Bronze: raw ingest — append only, no transform
df_raw.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_year", "ingestion_month", "ingestion_day") \
    .mode("append") \
    .save("s3://datalake/bronze/claims/")

# Silver: DQ + dedup + conform
silver_claims = (
    spark.read.format("delta").load("s3://datalake/bronze/claims/")
    .dropDuplicates(["claim_id", "source_system"])
    .filter("claim_amount > 0 AND patient_id IS NOT NULL")
    .withColumn("claim_date", to_date("claim_date_str", "yyyy-MM-dd"))
    .withColumn("state_code", upper(trim(col("state_code"))))
)

# Gold: business aggregation (also in dbt)
gold_payer_summary = (
    silver_claims
    .groupBy("payer_id", "claim_year", "claim_month")
    .agg(
        sum("claim_amount").alias("total_billed"),
        sum("paid_amount").alias("total_paid"),
        count("claim_id").alias("claim_count"),
        avg("processing_days").alias("avg_tat")
    )
)
```

### Tradeoffs
| Dimension | Two-tier (Lake + DWH) | Medallion Lakehouse |
|---|---|---|
| Data copies | Two (lake + warehouse) | One (single source of truth) |
| ETL complexity | Higher (two pipelines) | Lower (one pipeline) |
| Cost | Higher (two platforms) | Lower |
| BI performance | Excellent (DWH optimized) | Good (Delta + cluster) |
| ML access | Lake only | Same platform as BI |
| Governance | Harder (two platforms) | Unified (Unity Catalog) |

### Common Mistakes
- ❌ Putting business logic (currency conversion, derived KPIs) in Bronze — Bronze must be a pristine audit layer
- ❌ Schema enforcement in Bronze — Silver enforces schema; Bronze tolerates drift
- ❌ Skipping Silver — loading Bronze directly to Gold creates untrustworthy Gold tables
- ❌ One monolithic Gold table for all consumers — create separate Gold tables per use case (BI, ML, regulatory)
- ✅ DQ checks belong at the Bronze→Silver boundary (Great Expectations / dbt tests)
- ✅ Partition Bronze by ingestion date; partition Silver/Gold by business date

### Interview Questions
1. Why should Bronze never contain transformed data?
2. How do you handle schema drift in the Bronze layer?
3. What is the functional difference between Silver and Gold?
4. How would you implement data quality enforcement between Bronze and Silver?
5. How does Medallion architecture compare to a traditional Data Vault 2.0 layering approach?

---

## 5.2 Lambda vs Kappa Architecture

### Theory

**Lambda Architecture (Nathan Marz, 2011)**
Three layers to handle both historical and real-time data:

```
Source → ┌─── Batch Layer (Spark) ──────────► Batch Views ──┐
         │                                                    ├──► Serving Layer ──► Query
         └─── Speed Layer (Flink/Storm) ──► Speed Views ──────┘
```

- **Batch Layer:** Reprocesses all historical data. Accurate but high latency (hours-days).
- **Speed Layer:** Processes only recent data to compensate for batch latency. Near-real-time but approximate.
- **Serving Layer:** Merges batch + speed views. Query hits both and merges results.

**Problem:** Two separate code paths (batch + streaming) implementing the same business logic. Must maintain both. Consistency between layers is difficult. Extreme operational complexity.

---

**Kappa Architecture (Jay Kreps / LinkedIn, 2014)**
Remove the batch layer entirely. Everything is streaming.

```
Source → Kafka (immutable log, source of truth)
              │
              ├──► Stream Processor (Flink / Spark Streaming)
              │         ├──► Serving DB / Cache (Druid, Redis)
              │         └──► Delta/Iceberg (queryable history)
              │
              └──► Replay from offset 0 for reprocessing
                   (with new logic version → new output topic)
```

**Reprocessing in Kappa:** Deploy updated stream job → replay Kafka from earliest offset → write to new output. Eliminates batch layer. Single code path.

**Kappa's practical limitations:**
- Long historical replay from Kafka is expensive (compute + retention cost).
- Infinite Kafka retention is not free — costs scale with data volume.
- Complex stateful operations (sessionization, complex window aggregations) are harder in streaming than batch.
- Not all organizations have streaming-first maturity.

---

**Modern evolution: Lakehouse + Streaming (supersedes both)**
```
Source → Kafka → Flink / Spark Structured Streaming
                        │
                        ▼
              Apache Iceberg / Delta Lake
                        │
               ┌────────┴────────┐
               ▼                 ▼
           Trino/Athena      Flink streaming
           (batch queries)   (streaming queries)
```
- Iceberg/Delta provides time travel for reprocessing — no need to replay Kafka.
- Single data store serves both batch and streaming queries.
- Open table format = multi-engine access.

### Tradeoffs
| Dimension | Lambda | Kappa | Lakehouse + Stream |
|---|---|---|---|
| Code paths | Two (batch + stream) | One | One |
| Complexity | Very High | Medium | Medium |
| Latency | Near-RT (speed layer) | Near-RT | Near-RT |
| Reprocessing | Batch layer re-runs | Kafka replay (expensive) | Time travel (cheap) |
| Historical analytics | Excellent | Limited by retention | Excellent |
| Operational burden | Very High | Medium | Low-Medium |
| 2024 verdict | Legacy | Niche | **Preferred** |

### Common Mistakes
- ❌ Implementing Lambda when Kafka + Flink + Iceberg can replace it
- ❌ Choosing Kappa without accounting for Kafka storage cost at petabyte scale
- ❌ Forgetting that Kappa reprocessing requires re-consuming all Kafka data — slow for large histories
- ✅ Lambda still justified when: batch job uses fundamentally different algorithm than streaming (e.g., ML batch training + streaming inference)

### Interview Questions
1. Why did Kappa Architecture emerge as an alternative to Lambda?
2. What are the practical limitations of Kappa at petabyte scale?
3. How does a Lakehouse with streaming supersede both Lambda and Kappa?
4. In what scenario would you still recommend Lambda Architecture today?
5. How does Iceberg time travel replace Kafka replay for reprocessing?

---

## 5.3 Data Mesh

### Theory

**Problem Data Mesh solves:** Central data teams don't scale with organizational growth. The central data lake becomes a bottleneck — domain teams wait weeks for pipelines. Data quality ownership is diffuse.

**Four Principles (Zhamak Dehghani, 2019):**

**1. Domain Ownership**
Data owned and managed by the domain that produces it. "Claims" domain owns claims data end-to-end. Eliminates central data team as single bottleneck. Domain teams are accountable for quality.

**2. Data as a Product**
Domains treat data as products with consumers. A good data product has:
- SLAs (freshness, availability, accuracy)
- Documentation and schema contracts
- Versioning (backward-compatible changes)
- Owner accountability
- Discoverability via data catalog
Not a raw pipeline dump — a curated, reliable, governed asset.

**3. Self-Serve Data Platform**
Central platform team provides infrastructure tooling so domain teams can publish/consume data products without deep data engineering expertise. Domain teams don't need to know Kafka internals or Spark cluster tuning.

**4. Federated Computational Governance**
Global policies (GDPR, HIPAA, PII classification, access control) enforced centrally via automated platform controls. Applied locally per domain. Not enforced by humans in process — encoded in the platform (Unity Catalog, Apache Atlas, Collibra APIs).

### When Data Mesh Fits vs Fails
| Signal | Data Mesh | Central DWH |
|---|---|---|
| Org size | 500+ engineers, multiple domains | Any size |
| Central team bottleneck | Yes — domains waiting months | No |
| Domain engineering maturity | High (domains can build pipelines) | Low |
| Domain boundaries | Clear | Unclear |
| Self-serve platform investment | Committed | Not planned |
| Cross-domain analysis | Infrequent | Frequent |

### Tradeoffs
| Dimension | Data Mesh | Central DWH / Lake |
|---|---|---|
| Scalability | Domain-parallel | Central bottleneck |
| Data quality accountability | Domain owns it | Diffuse/unclear |
| Cross-domain analysis | Complex (federated joins) | Easy (centralized) |
| Governance | Federated (harder) | Centralized (simpler) |
| Implementation cost | Very high | Lower |
| Time to value | Long | Shorter |
| Organizational change | Massive | Minimal |

### Common Mistakes
- ❌ Implementing Data Mesh without a self-serve platform — domain teams drown in infrastructure
- ❌ Data Mesh in small orgs (< 100 engineers) — overhead outweighs benefit
- ❌ Treating Data Mesh as a technology pattern — it is an organizational and sociotechnical pattern
- ❌ No federated governance enforcement — domains produce incompatible schemas, PII exposed
- ✅ Start with 2-3 pilot domains before org-wide rollout
- ✅ Invest in data catalog (Collibra, DataHub) and platform team before domain teams produce data products

### Interview Questions
1. What problem does Data Mesh solve that a central data lake cannot?
2. What is a data product and what makes it "good"?
3. How does federated computational governance work technically?
4. Would you recommend Data Mesh for a 50-person startup? Why or why not?
5. How does Data Mesh relate to microservices architecture philosophically?

---

## 5.4 Data Fabric

### Theory

**Data Fabric** is an architecture and set of data services that provides consistent capabilities across a choice of endpoints spanning on-premises, cloud, and edge environments. Coined by Gartner.

**Key distinction from Data Mesh:**
- Data Mesh = organizational/sociotechnical pattern (domain ownership)
- Data Fabric = technology/integration pattern (unified metadata + access layer)

**Core components:**
- **Active Metadata Layer:** Continuously updated metadata from all data sources. ML-augmented — learns usage patterns, recommends pipelines, auto-classifies data.
- **Unified Data Access:** Single semantic layer over heterogeneous sources (cloud DWH, on-prem DB, data lake, SaaS). Users query through fabric without knowing underlying location.
- **Knowledge Graph:** Graph representation of metadata relationships — tables, columns, business terms, owners, lineage. Enables automated discovery.
- **Integrated Governance:** Policy enforcement across all endpoints from one control plane.

**Tools implementing Data Fabric concepts:**
- Microsoft Fabric (OneLake + unified governance)
- Informatica Intelligent Data Management Cloud (IDMC)
- IBM Watson Knowledge Catalog
- Denodo (virtual data fabric — query federation)

### Data Fabric vs Data Mesh
| Dimension | Data Fabric | Data Mesh |
|---|---|---|
| Primary focus | Technology integration layer | Organizational ownership model |
| Driven by | Tooling / vendors | Architecture philosophy |
| Key capability | Active metadata, ML-augmented discovery | Domain ownership, data products |
| Ownership model | Centralized (IT-driven) | Decentralized (domain-driven) |
| Complementary? | Yes — Data Fabric can be the platform for Data Mesh |

### Interview Questions
1. What is the difference between Data Fabric and Data Mesh?
2. What is active metadata and how does it differ from passive metadata?
3. How does Microsoft Fabric implement Data Fabric concepts?
4. Can Data Fabric and Data Mesh coexist? How?

---

## 5.5 Multi-Cloud Data Patterns

### Theory

**Why multi-cloud:**
- Avoid vendor lock-in
- Best-of-breed: AWS for ML (SageMaker), Azure for enterprise integration (ADF, Entra), GCP for BigQuery analytics
- Geographic data residency requirements
- M&A — acquired company on different cloud
- Negotiation leverage with cloud providers

**Key challenges:**
- Data egress costs (moving data between clouds is expensive — $0.08-0.09/GB)
- Latency between clouds (cross-region round trips)
- Consistent governance across clouds
- Different IAM models (AWS IAM vs Azure RBAC vs GCP IAM)
- Operational complexity

**Patterns:**

**Pattern 1 — Data Gravity (primary cloud + replication)**
One primary cloud holds all data. Only aggregated results replicated to secondary clouds. Minimize egress by keeping compute close to data.

**Pattern 2 — Open Table Format as Interoperability Layer**
Use Iceberg / Delta on S3 as the canonical store. Any engine on any cloud reads the same data. Snowflake, Athena, Databricks, BigQuery Omni all read Iceberg. True portability.

```
S3 (AWS) — Iceberg tables
     │
     ├──► Spark on EMR (AWS)
     ├──► Athena (AWS)
     ├──► Snowflake External Tables
     └──► BigQuery Omni (GCP reading AWS S3)
```

**Pattern 3 — Federated Query**
Query engine federates queries across clouds without moving data. Examples: Trino/Presto, Starburst, Dremio. Data stays in place; query results returned.

**Pattern 4 — Data Plane Separation**
Control plane centralized (governance, catalog, access control). Data plane distributed (storage in each cloud). Collibra / Purview as central governance over AWS + Azure + GCP data.

### AWS vs Azure vs GCP Data Engineering Services
| Service Category | AWS | Azure | GCP |
|---|---|---|---|
| Object Storage | S3 | ADLS Gen2 | GCS |
| Data Warehouse | Redshift | Synapse Analytics | BigQuery |
| Spark / Processing | EMR / Glue | HDInsight / ADF | Dataproc |
| Stream Processing | Kinesis | Event Hubs | Pub/Sub |
| Orchestration | MWAA (Airflow) | ADF / Fabric Pipelines | Cloud Composer |
| Catalog | Glue Catalog | Purview | Dataplex |
| ML Platform | SageMaker | Azure ML | Vertex AI |
| Lakehouse | EMR + Delta / Iceberg | Fabric / Synapse | BigLake + Iceberg |
| CDC | DMS | Data Factory CDC | Datastream |

### Egress Cost Strategy
```
Rule: Compute should live where data lives. Never move raw data between clouds.

If you must move:
  1. Aggregate first, move summary (not raw)
  2. Use cloud provider interconnect (lower egress than public internet)
  3. Use Iceberg on S3 as canonical — compute from any cloud without copying
  4. CloudFront / CDN for read-heavy cross-region access
```

### Common Mistakes
- ❌ Replicating full datasets between clouds — egress costs destroy ROI
- ❌ Inconsistent IAM across clouds — security gap at seam between clouds
- ❌ No unified data catalog across clouds — data discovery fails
- ❌ Different governance policies per cloud — GDPR/HIPAA violations at boundaries
- ✅ Use open table formats (Iceberg) as multi-cloud interoperability layer
- ✅ Centralize governance (Collibra / Purview) over all cloud data assets

### Interview Questions
1. What is data egress cost and how does it influence multi-cloud architecture decisions?
2. How does Apache Iceberg enable multi-cloud interoperability?
3. What is federated query and when would you use it vs data replication?
4. How do you maintain consistent governance across AWS, Azure, and GCP?
5. When would you recommend a single-cloud strategy over multi-cloud?

---

## 5.6 Microsoft Fabric Overview

### Theory

**Microsoft Fabric** (GA: November 2023) is Microsoft's unified analytics platform. One SaaS platform replacing the fragmented Azure data stack.

**Core components:**
- **OneLake:** Single logical data lake across all Fabric workloads. Delta Parquet format. One copy of data.
- **Data Factory:** Ingestion pipelines (replaces ADF in Fabric context).
- **Synapse Data Engineering:** Spark notebooks + Spark job definitions on OneLake.
- **Synapse Data Warehouse:** SQL-based DWH on OneLake (Delta tables).
- **Synapse Real-Time Analytics:** Kusto (KQL) engine for streaming/time-series.
- **Power BI:** Native BI. Direct Lake mode — reads Delta files directly, no import.
- **Data Science:** ML notebooks with MLflow integration.
- **Data Activator:** Event-driven alerting on streaming data.

**Key differentiator — Direct Lake mode:**
Power BI reads Delta files directly from OneLake. No import (no data copy). No DirectQuery latency. Near-import performance on live data. Revolutionary for BI-on-Lakehouse.

**Fabric vs Databricks:**
| Dimension | Microsoft Fabric | Databricks |
|---|---|---|
| Primary strength | BI + Lakehouse (Power BI native) | ML + Engineering (MLflow, Unity Catalog) |
| Compute engine | Spark + Kusto + SQL | Spark + Photon (vectorized) |
| Openness | Delta (Microsoft-controlled) | Delta (open-sourced) + Iceberg support |
| Governance | Purview integration | Unity Catalog |
| Best for | Azure-first, Power BI-heavy shops | ML-heavy, multi-cloud, engineering-first |

### Interview Questions
1. What is OneLake and how does it differ from ADLS Gen2?
2. What is Direct Lake mode in Power BI and why is it significant?
3. When would you choose Microsoft Fabric over Databricks?
4. How does Fabric's architecture relate to Medallion architecture?

---

*Next: Chunk 06 — Processing (ETL/ELT, Batch vs Streaming, CDC Internals, Event-Driven, Exactly-Once, Idempotency, Retry, Backpressure)*
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
# DE Master Doc — Chunk 07: Orchestration
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 7.1 DAG Concepts

### Theory

**DAG (Directed Acyclic Graph):** A graph of tasks where edges represent dependencies and no cycles exist. The foundational concept behind all workflow orchestrators.

**Key properties:**
- **Directed:** Dependencies flow in one direction (A → B means B depends on A)
- **Acyclic:** No circular dependencies (A → B → C → A is invalid — would loop forever)
- **Graph:** Multiple paths, parallel branches, fan-in, fan-out supported

**DAG components:**
- **Node/Task:** A unit of work (run SQL, trigger Spark job, call API, send email)
- **Edge/Dependency:** Defines execution order between tasks
- **Root node:** Task with no upstream dependencies (entry point)
- **Leaf node:** Task with no downstream dependencies (exit point)

**DAG execution patterns:**
```
Linear:          A → B → C → D

Fan-out:         A → B
                 A → C
                 A → D

Fan-in:          A → D
                 B → D
                 C → D

Diamond:         A → B → D
                 A → C → D

Complex:         A → B → E → G
                 A → C → F → G
                 A → D     → G
```

**Critical Path:** The longest path through the DAG. Determines minimum possible completion time. Optimizing the critical path = optimizing overall DAG runtime.

### Why DAGs (not pipelines or cron)?
```
Cron:      Fixed schedule, no dependency awareness, no retry, no monitoring
Pipeline:  Linear only, no fan-out parallelism
DAG:       Dependency-aware, parallel where possible, retry per task,
           monitoring per task, backfill, SLA alerting
```

---

## 7.2 Apache Airflow — Internals

### Theory

**Airflow components:**

| Component | Role |
|---|---|
| **Webserver** | Flask UI — DAG visualization, task logs, trigger, monitor |
| **Scheduler** | Parses DAG files, schedules task instances, sends to executor |
| **Executor** | Executes tasks. Pluggable: Local, Celery, Kubernetes, Sequential |
| **Metadata DB** | PostgreSQL/MySQL — stores DAG runs, task instances, connections, variables |
| **Worker** | (Celery/K8s) Processes executing tasks |
| **DAG Directory** | Python files defining DAGs. Scheduler parses continuously. |

**Airflow execution flow:**
```
1. Scheduler parses DAG files (every 30s by default)
2. Scheduler creates DagRun for each scheduled interval
3. Scheduler checks task dependencies → queues ready tasks
4. Executor picks up queued tasks → sends to workers
5. Workers execute tasks → report status to metadata DB
6. Scheduler updates task instance state
7. Webserver reads metadata DB → displays status
```

**Task states:**
```
none → scheduled → queued → running → success
                                    → failed → up_for_retry → queued (retry)
                                    → failed (max retries) → skipped
```

**Executors:**

| Executor | Parallelism | Setup | Best For |
|---|---|---|---|
| Sequential | 1 task at a time | None | Dev/testing only |
| Local | N tasks (multi-process) | Simple | Single-node, small scale |
| Celery | N tasks across N workers | Redis/RabbitMQ broker | Mid-scale production |
| Kubernetes | N tasks in K8s pods | K8s cluster | Large-scale, isolation |
| CeleryKubernetes | Hybrid | Complex | Mixed workloads |

### Airflow DAG — Production Example
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,          # each run independent
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60),
    'email_on_failure': True,
    'email': ['de-alerts@company.com'],
    'sla': timedelta(hours=4),         # alert if not done in 4h
}

with DAG(
    dag_id='claims_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',     # 2 AM UTC daily
    start_date=days_ago(1),
    catchup=False,                     # don't backfill missed runs
    max_active_runs=1,                 # no concurrent runs
    tags=['claims', 'production'],
) as dag:

    # Task 1: Extract from source
    extract_claims = PythonOperator(
        task_id='extract_claims_from_oracle',
        python_callable=extract_claims_fn,
        op_kwargs={'run_date': '{{ ds }}'},   # templated date
    )

    # Task 2a: Load raw to S3 (parallel with 2b)
    load_to_s3 = PythonOperator(
        task_id='load_raw_to_s3',
        python_callable=load_s3_fn,
    )

    # Task 2b: Validate schema (parallel with 2a)
    validate_schema = PythonOperator(
        task_id='validate_source_schema',
        python_callable=validate_fn,
    )

    # Task 3: Spark transform (after both 2a and 2b)
    spark_transform = EmrAddStepsOperator(
        task_id='spark_silver_transform',
        job_flow_id='{{ var.value.emr_cluster_id }}',
        steps=[{
            'Name': 'SilverTransform',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--py-files', 's3://code/transform.py']
            }
        }],
    )

    # Task 4: Load to Snowflake
    load_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake_gold',
        snowflake_conn_id='snowflake_prod',
        sql='CALL sp_load_fact_claims(\'{{ ds }}\');',
    )

    # Task 5: dbt run
    dbt_run = BashOperator(
        task_id='dbt_mart_refresh',
        bash_command='dbt run --select marts.claims --target prod',
    )

    # Task 6: Data quality check
    dq_check = PythonOperator(
        task_id='great_expectations_dq',
        python_callable=run_ge_checkpoint,
    )

    # Dependency graph
    extract_claims >> [load_to_s3, validate_schema]  # fan-out
    [load_to_s3, validate_schema] >> spark_transform  # fan-in
    spark_transform >> load_snowflake >> dbt_run >> dq_check
```

---

## 7.3 Scheduling Patterns

### Theory

**Cron expressions in Airflow:**
```
┌─── minute (0-59)
│  ┌─── hour (0-23)
│  │  ┌─── day of month (1-31)
│  │  │  ┌─── month (1-12)
│  │  │  │  ┌─── day of week (0-6, 0=Sunday)
│  │  │  │  │
*  *  *  *  *

Examples:
0 2 * * *        = 2:00 AM daily
0 6 * * 1        = 6:00 AM every Monday
*/15 * * * *     = every 15 minutes
0 0 1 * *        = midnight on 1st of every month
0 8,12,18 * * *  = 8 AM, 12 PM, 6 PM daily
```

**Airflow-specific schedule strings:**
```python
schedule_interval='@daily'    # midnight UTC
schedule_interval='@hourly'   # top of every hour
schedule_interval='@weekly'   # midnight Sunday
schedule_interval=None        # manual trigger only
schedule_interval=timedelta(hours=6)  # every 6 hours
```

**Data interval concept (Airflow 2.2+):**
Airflow runs AFTER the interval completes. `schedule='@daily'` with `start_date=2024-01-01` runs at 2024-01-02 00:00 to process 2024-01-01's data.

```
Logical date (data_interval_start): 2024-01-01
Execution date (actual run):        2024-01-02 00:00
Data interval:                      2024-01-01 00:00 → 2024-01-02 00:00

Template variables:
  {{ ds }}           = 2024-01-01 (logical date, YYYY-MM-DD)
  {{ data_interval_start }} = 2024-01-01T00:00:00
  {{ data_interval_end }}   = 2024-01-02T00:00:00
```

**Event-based triggering (Airflow 2.0+):**
```python
# TriggerDagRunOperator: DAG A triggers DAG B on completion
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_downstream = TriggerDagRunOperator(
    task_id='trigger_reporting_dag',
    trigger_dag_id='claims_reporting_pipeline',
    wait_for_completion=True,           # wait for triggered DAG
    conf={'run_date': '{{ ds }}'},      # pass context
)

# Dataset-based scheduling (Airflow 2.4+)
from airflow.datasets import Dataset

claims_dataset = Dataset("s3://datalake/silver/claims/")

# Producer DAG marks dataset as updated
with DAG(..., schedule='@daily') as producer_dag:
    load_task = PythonOperator(...)
    load_task.outlets = [claims_dataset]   # marks dataset updated

# Consumer DAG triggers when dataset is updated
with DAG(..., schedule=[claims_dataset]) as consumer_dag:
    ...  # runs automatically when claims_dataset is updated
```

---

## 7.4 Dependency Management

### Theory

**Task-level dependencies:**
```python
# Bitshift operator (most common)
task_a >> task_b >> task_c           # linear
task_a >> [task_b, task_c]           # fan-out
[task_b, task_c] >> task_d           # fan-in

# set_upstream / set_downstream (equivalent)
task_b.set_upstream(task_a)
task_c.set_downstream(task_d)
```

**Cross-DAG dependencies:**

**Pattern 1: ExternalTaskSensor**
```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_extract_dag',
    external_dag_id='extract_pipeline',
    external_task_id='final_task',
    allowed_states=['success'],
    execution_delta=timedelta(hours=1),    # upstream runs 1h earlier
    timeout=3600,                           # fail after 1 hour waiting
    poke_interval=60,                       # check every 60s
    mode='reschedule',                      # release worker slot while waiting
)
```

**Pattern 2: Dataset-based (Airflow 2.4+)** ← preferred
```python
# No polling. Event-driven. DAG runs when upstream marks dataset.
with DAG(schedule=[upstream_dataset]) as dag:
    ...
```

**Trigger rules (dependency logic):**
```python
from airflow.utils.trigger_rule import TriggerRule

# Default: all_success — run only if ALL upstream tasks succeeded
task = PythonOperator(trigger_rule=TriggerRule.ALL_SUCCESS, ...)

# all_done — run regardless of upstream success/failure
cleanup = PythonOperator(trigger_rule=TriggerRule.ALL_DONE, ...)

# one_success — run if ANY upstream task succeeded
# one_failed — run if ANY upstream task failed (alerting)
# none_failed — run if no upstream tasks failed (skipped OK)

# Common pattern: cleanup always runs, alert on failure
alert_task = PythonOperator(
    task_id='send_failure_alert',
    trigger_rule=TriggerRule.ONE_FAILED,
    python_callable=send_slack_alert,
)
[task_a, task_b, task_c] >> alert_task
```

**XCom (Cross-Communication between tasks):**
```python
# Push value from task
def extract_fn(**context):
    record_count = run_extract()
    context['ti'].xcom_push(key='record_count', value=record_count)

# Pull value in downstream task
def validate_fn(**context):
    count = context['ti'].xcom_pull(
        task_ids='extract_claims',
        key='record_count'
    )
    if count == 0:
        raise ValueError("No records extracted — aborting pipeline")
```

**XCom limitations:**
- Stored in Airflow metadata DB → not for large data
- Max size: ~48KB (SQLite), larger with PostgreSQL but still limited
- For large data: write to S3/GCS, pass the path via XCom

---

## 7.5 Failure Recovery

### Theory

**Failure types and recovery strategies:**

| Failure Type | Example | Recovery |
|---|---|---|
| Transient failure | Network timeout, API rate limit | Retry with backoff |
| Task failure | Bad data, assertion error | Fix + clear task + re-run |
| DAG failure | Dependency never satisfied | Investigate upstream |
| Infra failure | Worker node crash | Re-queue task automatically |
| Data failure | Wrong data in target | Backfill from correct source |

**Airflow recovery mechanisms:**

**1. Task-level retry**
```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}
```

**2. Clear and re-run**
```bash
# Clear specific task instance (reset to None state → re-run)
airflow tasks clear claims_daily_pipeline \
    --task-id spark_silver_transform \
    --start-date 2024-01-15 \
    --end-date 2024-01-15

# Clear entire DAG run
airflow dags clear claims_daily_pipeline \
    --start-date 2024-01-15
```

**3. Backfill (catch-up missed runs)**
```bash
# Run DAG for missed historical dates
airflow dags backfill claims_daily_pipeline \
    --start-date 2024-01-01 \
    --end-date 2024-01-10

# Important: pipeline must be idempotent for backfill to be safe
# Re-running 2024-01-05 must produce same result as original run
```

**4. SLA Miss alerting**
```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_pagerduty_alert(f"SLA missed: {dag.dag_id}")

with DAG(
    ...,
    sla_miss_callback=sla_miss_callback,
    default_args={'sla': timedelta(hours=6)},
) as dag:
    ...
```

**5. Dead letter / failed task notification**
```python
def failure_callback(context):
    task_instance = context['task_instance']
    send_slack_message(
        channel='#data-alerts',
        message=f"❌ Task failed: {task_instance.dag_id}.{task_instance.task_id}\n"
                f"Date: {context['ds']}\n"
                f"Log: {task_instance.log_url}"
    )

default_args = {
    'on_failure_callback': failure_callback,
}
```

**Idempotency requirement for failure recovery:**
```
Backfill and retry ONLY work correctly if pipeline is idempotent.
Running the same DAG for 2024-01-05 twice must produce identical output.

Idempotency patterns:
  • MERGE instead of INSERT (upsert, no duplicates on retry)
  • Partition overwrite: spark.write.mode("overwrite").partitionBy("date")
  • Truncate + reload per partition (DELETE WHERE date=X, then INSERT)
  • dbt incremental with unique_key (deduplicates on re-run)
```

### Common Airflow Mistakes
- ❌ `catchup=True` on a new DAG with old `start_date` — triggers hundreds of backfill runs immediately
- ❌ Long-running tasks in the Airflow scheduler process — scheduler should only orchestrate, not process
- ❌ Large data in XCom — metadata DB bloat, slow Airflow UI
- ❌ Dynamic task generation with complex Python in DAG file — scheduler parses every 30s, slow parsing = scheduler lag
- ❌ No SLA defined — silent delays go undetected for hours
- ❌ `depends_on_past=True` without understanding implication — one failure blocks all future runs for that DAG
- ✅ Keep DAG files lightweight — no DB queries, no heavy imports at module level
- ✅ Use `catchup=False` for new production DAGs
- ✅ Set `max_active_runs=1` to prevent concurrent runs on daily pipelines
- ✅ `mode='reschedule'` on sensors — releases worker slot while waiting (vs `mode='poke'` which holds it)

### Interview Questions — Full Set

**DAG + Airflow Architecture:**
1. What components make up Apache Airflow and what does each do?
2. How does the Airflow scheduler decide when to queue a task?
3. What is the difference between CeleryExecutor and KubernetesExecutor?
4. What happens when the Airflow metadata DB goes down?

**Scheduling:**
5. What is the difference between `execution_date` and `data_interval_start` in Airflow 2.x?
6. How does Airflow's `catchup=True` work and when is it dangerous?
7. What is dataset-based scheduling in Airflow 2.4+ and how does it differ from ExternalTaskSensor?

**Dependencies:**
8. What trigger rules does Airflow support and when would you use `ONE_FAILED`?
9. What is XCom and what are its limitations?
10. How do you implement cross-DAG dependencies in Airflow?

**Failure Recovery:**
11. What is the difference between clearing a task and re-triggering a DAG?
12. How do you backfill missed DAG runs and what prerequisite must your pipeline meet?
13. What is `depends_on_past` and when would enabling it cause problems?
14. How do you implement SLA alerting in Airflow?

**Production:**
15. What is the small scheduler problem in Airflow and how do you fix it?
16. Why should DAG files not contain DB connections or heavy imports at module level?
17. How does `mode='reschedule'` on sensors improve Airflow scalability vs `mode='poke'`?

---

## 7.6 Airflow vs Alternatives

### Comparison
| Dimension | Airflow | Prefect | Dagster | dbt Cloud |
|---|---|---|---|---|
| Primary use | General orchestration | Python-native workflows | Data-aware orchestration | dbt-specific |
| DAG definition | Python (decorator/class) | Python (flow/task) | Python (assets/ops) | YAML + SQL |
| Data awareness | Limited | Limited | First-class (assets) | dbt models |
| UI quality | Functional | Modern | Excellent | Good |
| Dynamic DAGs | Complex | Native | Native | Limited |
| Cloud managed | MWAA, Astronomer, GCC | Prefect Cloud | Dagster Cloud | dbt Cloud |
| Learning curve | High | Medium | High | Low (SQL) |
| Best for | Enterprise, complex deps | Python-heavy teams | Asset-centric workflows | dbt shops |

**Dagster's Software-Defined Assets (SDA) — the modern approach:**
```python
# Dagster: define assets (not tasks)
from dagster import asset, AssetIn

@asset
def bronze_claims():
    """Raw claims data from source"""
    return extract_and_land_raw()

@asset(ins={"bronze": AssetIn("bronze_claims")})
def silver_claims(bronze):
    """Cleansed claims data"""
    return apply_dq_rules(bronze)

@asset(ins={"silver": AssetIn("silver_claims")})
def gold_claims_mart(silver):
    """Business-ready claims mart"""
    return build_star_schema(silver)

# Dagster tracks lineage automatically
# Re-materialize any asset independently
# Observe data freshness per asset
```

### Interview Questions
1. When would you choose Dagster over Airflow?
2. What is a Software-Defined Asset in Dagster and how does it differ from an Airflow task?
3. What is the difference between Prefect flows and Airflow DAGs?
4. How does dbt Cloud scheduling compare to Airflow for dbt-only pipelines?

---

*Next: Chunk 08 — Cloud Data Engineering (Snowflake deep dive, BigQuery internals, Databricks architecture, Synapse/Fabric, AWS/Azure/GCP comparison)*
# DE Master Doc — Chunk 08: Cloud Data Engineering
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 8.1 Snowflake — Deep Architecture

### Theory

**Three-tier architecture (the key differentiator):**

```
┌──────────────────────────────────────────────────┐
│  CLOUD SERVICES LAYER (Brain — always on)        │
│  • Authentication + access control               │
│  • Query parsing, optimization, compilation      │
│  • Transaction management (ACID)                 │
│  • Metadata management (table stats, schema)     │
│  • Micro-partition pruning decisions             │
└──────────────────────────────────────────────────┘
                        ↕ (compute requests)
┌──────────────────────────────────────────────────┐
│  QUERY PROCESSING LAYER (Virtual Warehouses)     │
│  • Ephemeral compute clusters (spin up/down)     │
│  • Each VW = N nodes with local SSD cache        │
│  • Multiple VWs run on SAME data (no contention) │
│  • Scale UP: larger node size (XS→XL→4XL)       │
│  • Scale OUT: multi-cluster (auto-scale)         │
└──────────────────────────────────────────────────┘
                        ↕ (storage reads)
┌──────────────────────────────────────────────────┐
│  DATABASE STORAGE LAYER (S3 / Azure Blob / GCS)  │
│  • Columnar micro-partitioned data               │
│  • Automatic compression (5-10x)                 │
│  • Shared across ALL virtual warehouses          │
│  • No data copying between VWs                   │
└──────────────────────────────────────────────────┘
```

**Micro-partitions:**
- Automatically organized 50-500MB compressed columnar chunks.
- Each micro-partition stores column-level statistics: min, max, null_count, distinct_count.
- Cloud services layer uses statistics for pruning BEFORE sending to VW.
- Query touches only micro-partitions where min ≤ filter_value ≤ max.
- No manual partition management required (unlike Hive).

**Caching hierarchy (3 levels):**
```
Level 1 — Result Cache (Cloud Services Layer):
  • Exact query result cached for 24 hours
  • If same query re-runs AND underlying data unchanged → instant return
  • ZERO compute cost (VW not even started)
  • Invalidated when referenced table data changes

Level 2 — Local Disk Cache (Virtual Warehouse SSD):
  • Column data cached on VW node SSD after first S3 read
  • Survives across queries while VW stays running
  • Lost when VW is suspended/resized
  • Check: Query Profile → "% scanned from cache"

Level 3 — Remote Storage (S3/Azure/GCS):
  • Slowest. Accessed only on cache miss.
  • All VWs share the same storage — no ownership
```

**Snowflake features — architect level:**

**Time Travel:**
- Access historical data at any point within retention period (1–90 days, Enterprise+).
- Uses transaction log — no physical copies, metadata-driven.
```sql
-- Query data as of 1 hour ago
SELECT * FROM fact_claims AT (OFFSET => -3600);

-- Query data at specific timestamp
SELECT * FROM fact_claims AT (TIMESTAMP => '2024-01-15 09:00:00'::TIMESTAMP);

-- Query data at specific transaction ID
SELECT * FROM fact_claims AT (STATEMENT => '<query_id>');

-- Restore accidentally dropped/modified table
CREATE TABLE fact_claims_restored CLONE fact_claims
    AT (TIMESTAMP => '2024-01-15 08:00:00'::TIMESTAMP);
```

**Zero-Copy Cloning:**
- Instant clone of table/schema/database. No data copied.
- Clone shares micro-partitions with source. Diverges only as changes occur.
- Use: dev/test environments, pre-migration snapshots.
```sql
-- Instant clone — completes in seconds regardless of table size
CREATE DATABASE dev_env CLONE prod_env;
CREATE TABLE claims_backup CLONE fact_claims;
```

**Streams + Tasks (CDC within Snowflake):**
```sql
-- Stream: tracks DML changes on source table
CREATE STREAM claims_stream ON TABLE claims_raw;
-- Stream captures: INSERT, UPDATE, DELETE with METADATA$ACTION column

-- Task: scheduled processing of stream
CREATE TASK process_claims_stream
    WAREHOUSE = transform_wh
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('claims_stream')
AS
    MERGE INTO fact_claims t
    USING (SELECT * FROM claims_stream WHERE METADATA$ACTION = 'INSERT') s
    ON t.claim_id = s.claim_id
    WHEN MATCHED THEN UPDATE SET ...
    WHEN NOT MATCHED THEN INSERT ...;

ALTER TASK process_claims_stream RESUME;
```

**Dynamic Data Masking:**
```sql
-- Masking policy: hide SSN from non-privileged users
CREATE MASKING POLICY ssn_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() IN ('PHI_ANALYST', 'DATA_ENGINEER') THEN val
        ELSE '***-**-' || RIGHT(val, 4)
    END;

ALTER TABLE dim_patient
    MODIFY COLUMN ssn SET MASKING POLICY ssn_mask;
```

**Row Access Policies (Row-Level Security):**
```sql
-- Only see claims for your assigned payer
CREATE ROW ACCESS POLICY payer_filter AS (payer_id VARCHAR) RETURNS BOOLEAN ->
    payer_id IN (
        SELECT payer_id FROM user_payer_mapping
        WHERE username = CURRENT_USER()
    );

ALTER TABLE fact_claims ADD ROW ACCESS POLICY payer_filter ON (payer_id);
```

### Production Tuning
```sql
-- 1. Resource monitor: cap credit spend
CREATE RESOURCE MONITOR prod_monitor
    WITH CREDIT_QUOTA = 500
    TRIGGERS ON 80 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND;

-- 2. Auto-suspend idle warehouses (save cost)
ALTER WAREHOUSE analytics_wh
    SET AUTO_SUSPEND = 60     -- seconds idle before suspend
        AUTO_RESUME  = TRUE;  -- auto-start on query

-- 3. Query tagging for cost attribution
ALTER SESSION SET QUERY_TAG = 'team=claims,pipeline=daily_load';

-- 4. Clustering: identify over-clustered tables
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_claims', '(claim_date, payer_id)');
-- average_depth close to 1.0 = well clustered
-- average_depth > 6 = needs re-clustering

-- 5. Search Optimization Service: point lookup acceleration
ALTER TABLE dim_patient ADD SEARCH OPTIMIZATION ON EQUALITY(patient_nk, ssn);
```

### Common Mistakes
- ❌ Running large transformations on XS warehouse — wrong size = slow; scale UP not OUT for single large queries
- ❌ Scale OUT (multi-cluster) for one big query — multi-cluster helps concurrency, not single query speed
- ❌ Forgetting to suspend warehouses — idle VW continues billing credits
- ❌ Using Time Travel as backup strategy — it's not a backup; FAIL_SAFE is the 7-day recovery layer
- ❌ Clustering on low-cardinality columns — wastes auto-cluster credits with no pruning benefit
- ✅ Query Profile is your first debugging tool — check: spillage to disk, poor join order, missing pruning
- ✅ Use `COPY INTO` for bulk loading (not INSERT row by row) — 100x faster

### Interview Questions
1. What is Snowflake's shared-disk architecture and how does it differ from shared-nothing?
2. How does Snowflake's result cache work and when is it invalidated?
3. What is a virtual warehouse in Snowflake and when do you scale up vs scale out?
4. How do Snowflake Streams and Tasks implement CDC without Debezium?
5. What is zero-copy cloning and what are its storage implications?
6. How does Snowflake micro-partition pruning differ from Hive partitioning?
7. What is the difference between Time Travel and Fail-Safe in Snowflake?

---

## 8.2 BigQuery — Internals

### Theory

**BigQuery architecture:**
- Serverless: no cluster to manage. Google manages compute allocation.
- Dremel engine: in-situ query engine that reads columnar data directly from Colossus (Google's distributed file system).
- Slot-based compute: slots = units of BigQuery compute. On-demand = up to 2000 slots. Flat-rate = reserved slots.
- Separation of storage and compute (like Snowflake but serverless).

**Storage:**
- Capacitor format: BigQuery's proprietary columnar format on Colossus.
- Automatic columnar compression + encoding.
- Automatically partitioned by ingestion time (default) or custom column.
- Streaming buffer: new streamed data lands here first, queryable immediately.

**Partitioning in BigQuery:**
```sql
-- Date/timestamp partitioning
CREATE TABLE claims_partitioned
PARTITION BY DATE(claim_date)
OPTIONS (
    partition_expiration_days = 365,
    require_partition_filter = TRUE    -- force WHERE claim_date filter
)
AS SELECT * FROM raw_claims;

-- Integer range partitioning
CREATE TABLE claims_by_payer
PARTITION BY RANGE_BUCKET(payer_id, GENERATE_ARRAY(0, 1000, 10));

-- Ingestion-time partitioning (default if no column specified)
CREATE TABLE claims_ingested
PARTITION BY _PARTITIONDATE;
```

**Clustering in BigQuery:**
```sql
-- Partition + cluster (up to 4 cluster columns)
CREATE TABLE claims_optimized
PARTITION BY DATE(claim_date)
CLUSTER BY payer_id, diagnosis_code, state_code
AS SELECT * FROM raw_claims;
-- Cost: Clustering in BQ is FREE (included in slot cost)
```

**Cost model:**
- On-demand: $6.25 per TB scanned (as of 2024). Pay per query.
- Flat-rate: Reserved slots. Predictable cost for high-volume workloads.
- Storage: $0.02/GB/month (active), $0.01/GB/month (long-term after 90 days no edit).

**BigQuery ML (BQML):**
```sql
-- Train model directly in SQL
CREATE MODEL claims.fraud_detection_model
OPTIONS (
    model_type = 'LOGISTIC_REG',
    input_label_cols = ['is_fraudulent']
)
AS
SELECT diagnosis_code, payer_id, billed_amount, processing_days, is_fraudulent
FROM claims.fact_claims_training;

-- Predict without leaving BigQuery
SELECT * FROM ML.PREDICT(
    MODEL claims.fraud_detection_model,
    (SELECT * FROM claims.fact_claims_new)
);
```

**BigQuery vs Snowflake:**
| Dimension | BigQuery | Snowflake |
|---|---|---|
| Compute model | Serverless (slots) | Virtual warehouses (managed) |
| Pricing | Per TB scanned | Per compute second |
| Cost predictability | Variable | Predictable (fixed VW size) |
| Concurrency | Very high (serverless) | Limited by VW concurrency |
| ML integration | Native (BQML) | Cortex AI (growing) |
| Multi-cloud | GCP only | AWS + Azure + GCP |
| Time travel | 7 days | 1–90 days (Enterprise) |
| Data sharing | Analytics Hub | Snowflake Marketplace |
| Best for | GCP-native, variable workloads | Multi-cloud, SaaS, regulated |

### Common Mistakes
- ❌ Querying unpartitioned tables without filters — scans entire table, high cost
- ❌ `SELECT *` on large tables — BQ charges by bytes scanned; always select needed columns
- ❌ Not using `require_partition_filter` — users accidentally scan full table history
- ❌ Streaming inserts for bulk load — use `LOAD DATA` / `bq load` for batch; streaming is expensive
- ✅ Always preview estimated bytes scanned before running expensive queries
- ✅ Use `INFORMATION_SCHEMA.JOBS` to audit query costs by user/team

### Interview Questions
1. How does BigQuery's serverless model differ from Snowflake's virtual warehouse model?
2. What is a BigQuery slot and how does flat-rate pricing work?
3. How does BigQuery clustering differ from Snowflake clustering in cost and implementation?
4. What is the streaming buffer in BigQuery and what are its limitations?
5. When would you choose BigQuery over Snowflake?

---

## 8.3 Databricks — Architecture

### Theory

**Databricks = Unified Analytics Platform on top of Apache Spark + Delta Lake.**

**Core architecture:**
```
┌──────────────────────────────────────────────────────┐
│  CONTROL PLANE (Databricks-managed)                  │
│  • Web UI, REST API, Jobs scheduler                  │
│  • Cluster manager, notebook server                  │
│  • Unity Catalog (governance)                        │
│  • MLflow (experiment tracking)                      │
└──────────────────────────────────────────────────────┘
                        ↕
┌──────────────────────────────────────────────────────┐
│  DATA PLANE (Customer's cloud account)               │
│  • Spark clusters (EC2/AKS/GCE instances)            │
│  • Delta Lake tables on S3/ADLS/GCS                  │
│  • Data stays in YOUR cloud account                  │
└──────────────────────────────────────────────────────┘
```

**Cluster types:**
- **All-purpose clusters:** Interactive notebooks. Long-running. Expensive. For development/exploration.
- **Job clusters:** Ephemeral. Start for job, terminate after. Cheapest. For production pipelines.
- **SQL Warehouses:** Serverless SQL compute for Databricks SQL (BI workloads on Delta tables).

**Photon Engine:**
- Databricks-native vectorized query engine written in C++.
- Replaces Spark JVM execution with native columnar execution.
- 2-8x faster on SQL and Delta operations.
- Transparent — use same Spark SQL, runs on Photon automatically.
- Available on all-purpose and SQL warehouse clusters.

**Unity Catalog:**
- Unified governance layer across all Databricks workspaces.
- Three-level namespace: `catalog.schema.table`
- Column-level lineage across all Delta tables, notebooks, jobs.
- RBAC + ABAC on catalogs, schemas, tables, columns.
- Connects to external catalogs (Glue, Hive Metastore).

**Delta Live Tables (DLT) — declarative pipelines:**
```python
import dlt
from pyspark.sql.functions import *

# Bronze: raw ingest
@dlt.table(comment="Raw claims from Kafka")
def bronze_claims():
    return (
        spark.readStream.format("kafka")
        .option("subscribe", "claims-topic")
        .load()
    )

# Silver: cleansed
@dlt.table(comment="Validated and cleansed claims")
@dlt.expect("valid_amount", "claim_amount > 0")          # DQ rule
@dlt.expect_or_drop("non_null_id", "claim_id IS NOT NULL") # drop bad rows
def silver_claims():
    return (
        dlt.read_stream("bronze_claims")
        .withColumn("claim_date", to_date("claim_date_str"))
        .dropDuplicates(["claim_id"])
    )

# Gold: aggregation
@dlt.table(comment="Monthly claims summary by payer")
def gold_claims_summary():
    return (
        dlt.read("silver_claims")
        .groupBy("payer_id", year("claim_date"), month("claim_date"))
        .agg(sum("claim_amount"), count("claim_id"))
    )
```

**MLflow integration:**
```python
import mlflow
from mlflow.models.signature import infer_signature

with mlflow.start_run():
    model = train_fraud_model(X_train, y_train)

    mlflow.log_params({"n_estimators": 100, "max_depth": 5})
    mlflow.log_metrics({"auc": 0.94, "precision": 0.89})
    mlflow.sklearn.log_model(
        model,
        "fraud_model",
        signature=infer_signature(X_train, model.predict(X_train)),
        registered_model_name="FraudDetectionV2"
    )
```

### Databricks vs Snowflake
| Dimension | Databricks | Snowflake |
|---|---|---|
| Primary strength | ML + Data Engineering | SQL Analytics + BI |
| Language | Python/Scala/SQL | SQL-first |
| Streaming | Native Spark Streaming | Snowpipe (micro-batch) |
| ML Platform | MLflow native | Cortex AI (growing) |
| Governance | Unity Catalog | RBAC + masking + RLS |
| Concurrency model | Cluster-based | Virtual warehouse |
| Table format | Delta Lake (primary) | Snowflake native + Iceberg |
| Best for | ML-heavy, streaming, Python | BI, reporting, governed SQL |

### Common Mistakes
- ❌ All-purpose cluster for production jobs — expensive, not auto-terminated
- ❌ Not enabling auto-termination on all-purpose clusters — forgotten clusters bill continuously
- ❌ Spark `collect()` on large DataFrames — pulls all data to driver → OOM
- ❌ UDFs in Python Spark — serialization overhead; use native Spark functions instead
- ❌ Not using Delta Lake on Databricks — raw Parquet loses ACID, time travel, MERGE
- ✅ Job clusters for all production workloads — terminate after job, no idle billing
- ✅ Use `cache()` / `persist()` strategically on DataFrames reused across multiple actions

### Interview Questions
1. What is the difference between Databricks control plane and data plane?
2. What is the Photon engine and what type of workloads benefit most?
3. What is Delta Live Tables and how does it differ from raw Spark pipelines?
4. When would you choose Databricks over Snowflake for a new project?
5. What is Unity Catalog and what governance capabilities does it provide?
6. What is MLflow and how does it integrate with Databricks for ML lifecycle management?

---

## 8.4 AWS vs Azure vs GCP — Data Engineering Comparison

### Full Services Comparison
| Category | AWS | Azure | GCP |
|---|---|---|---|
| Object Storage | S3 | ADLS Gen2 / Blob Storage | GCS |
| Data Warehouse | Redshift | Synapse Analytics | BigQuery |
| Lakehouse | EMR + Delta/Iceberg | Fabric / Synapse Spark | BigLake + Iceberg |
| Managed Spark | EMR | HDInsight / Synapse Spark / Fabric | Dataproc |
| Serverless ETL | AWS Glue | ADF | Dataflow (Beam) |
| Stream Processing | Kinesis Data Streams | Event Hubs | Pub/Sub |
| Stream Analytics | Kinesis Data Analytics (Flink) | Azure Stream Analytics | Dataflow |
| Orchestration | MWAA (Managed Airflow) | ADF Pipelines / Fabric | Cloud Composer (Airflow) |
| Data Catalog | AWS Glue Catalog | Microsoft Purview | Dataplex |
| CDC | AWS DMS | ADF CDC / Fabric | Datastream |
| ML Platform | SageMaker | Azure Machine Learning | Vertex AI |
| Real-time DB | DynamoDB | Cosmos DB | Bigtable / Firestore |
| Message Queue | SQS / SNS | Service Bus | Pub/Sub |
| Secrets | Secrets Manager | Key Vault | Secret Manager |

### Decision Guide
```
Choose AWS when:
  • Broadest service ecosystem needed
  • Kafka → MSK (managed)
  • SageMaker for ML is a priority
  • Existing AWS infrastructure

Choose Azure when:
  • Microsoft-heavy shop (Office 365, Active Directory)
  • Power BI is primary BI tool (Fabric integration)
  • Informatica IICS + Azure ADF integration
  • Enterprise compliance: Azure Policy + Purview + Entra ID

Choose GCP when:
  • BigQuery is primary DWH (serverless, best-in-class SQL analytics)
  • Kubernetes-heavy (GKE is most mature K8s)
  • ML/AI is primary focus (Vertex AI, TPUs)
  • Data-intensive analytics at Google scale
```

### AWS Glue vs Azure ADF vs GCP Dataflow
| Dimension | AWS Glue | Azure ADF | GCP Dataflow |
|---|---|---|---|
| Engine | Spark (serverless) | SSIS + Spark + Data Flows | Apache Beam |
| Pricing | Per DPU-hour | Per activity run + DIU | Per vCPU-hour |
| Best for | S3 → Redshift/Athena | Azure ecosystem integration | Streaming (Beam unified) |
| Code | PySpark / Scala | GUI + JSON + Mapping DFs | Java / Python (Beam) |
| Streaming | Limited | Yes (streaming datasets) | Excellent (Beam unified batch+stream) |

### Common Mistakes
- ❌ Choosing cloud platform based on hype vs actual workload fit
- ❌ Mixing clouds without egress cost analysis — data movement = unexpected bills
- ❌ Using cloud-native proprietary formats (Redshift Spectrum only, Synapse only) — lock-in
- ✅ Use open formats (Parquet + Iceberg) to preserve multi-cloud optionality
- ✅ Evaluate: existing team skills + existing infrastructure + primary workload type

### Interview Questions
1. When would you recommend Azure Synapse vs Databricks for a new project?
2. What is the key difference between AWS Glue and Azure Data Factory architecturally?
3. How does GCP Dataflow's Apache Beam model support both batch and streaming?
4. What are the cost implications of choosing Redshift vs BigQuery for unpredictable query workloads?
5. How would you design a multi-cloud data architecture to avoid vendor lock-in?

---

## 8.5 Azure Synapse Analytics + Microsoft Fabric

### Synapse Analytics
**What it is:** Unified analytics service combining: SQL pools (DWH), Spark pools (big data), Synapse Pipelines (ETL), Synapse Link (CDC from Cosmos DB).

**Two SQL pool types:**
- **Dedicated SQL Pool:** Traditional MPP DWH. Pre-allocated compute (DWUs). Best for consistent high-performance SQL analytics. Runs on distributions (hash or round-robin). Supports PolyBase for external data access.
- **Serverless SQL Pool:** On-demand queries over ADLS Gen2/Parquet/Delta/Iceberg. Pay per TB scanned. No loading needed. Good for data lake exploration.

**Synapse vs Databricks (Azure context):**
| Dimension | Synapse | Databricks |
|---|---|---|
| SQL DWH | Dedicated SQL Pool (MPP) | Delta Lake via SQL Warehouse |
| Spark | Synapse Spark Pools | Databricks Runtime (faster) |
| ML | Azure ML integration | MLflow native |
| Streaming | Synapse Link + Event Hubs | Spark Structured Streaming |
| Best for | SQL-heavy analytics teams | ML + engineering teams |

### Microsoft Fabric
**What it is:** Unified SaaS analytics platform (GA Nov 2023). Replaces fragmented Azure data stack.

```
OneLake (single logical lake — Delta Parquet format)
    │
    ├── Data Factory (ingestion)
    ├── Synapse Data Engineering (Spark notebooks)
    ├── Synapse Data Warehouse (SQL DWH on OneLake)
    ├── Synapse Real-Time Analytics (Kusto/KQL)
    ├── Power BI (Direct Lake mode)
    ├── Data Science (MLflow notebooks)
    └── Data Activator (streaming alerts)
```

**Direct Lake mode (key differentiator):**
Power BI reads Delta files directly from OneLake. No import (no data copy into Power BI model). No DirectQuery latency overhead. Import-speed performance on live Delta data. Eliminates scheduled dataset refresh.

### Interview Questions
1. What is the difference between Synapse Dedicated SQL Pool and Serverless SQL Pool?
2. What is Microsoft Fabric's OneLake and how does it unify the Azure data stack?
3. What is Direct Lake mode in Power BI and why is it significant for Lakehouse + BI architectures?
4. When would you recommend Microsoft Fabric over Databricks for an Azure-first organization?

---

*Next: Chunk 09 — Performance Optimization (Query optimization, Join strategies, Indexing, Predicate pushdown, Caching, Statistics, Cost optimization)*
# DE Master Doc — Chunk 09: Performance Optimization
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 9.1 Query Optimization

### Theory

**Query execution lifecycle:**
```
SQL Text
  → Parser (syntax tree)
  → Analyzer (semantic validation, resolve names)
  → Logical Plan (relational algebra)
  → Optimizer (rule-based + cost-based rewriting)
  → Physical Plan (join algorithms, scan methods)
  → Execution Engine (vectorized / interpreted)
  → Result
```

**Rule-Based Optimization (RBO):**
Fixed transformation rules applied regardless of data statistics.
- Predicate pushdown (filter early)
- Projection pruning (drop unused columns early)
- Constant folding (`WHERE 1+1=2` → `WHERE TRUE`)
- Subquery unnesting (correlated → join)

**Cost-Based Optimization (CBO):**
Uses table statistics (row count, column cardinality, histogram) to estimate cost of each plan and choose the cheapest.
- Join order selection (which table to drive from)
- Join algorithm selection (hash vs merge vs nested loop)
- Broadcast vs shuffle join decision
- Requires up-to-date statistics — stale stats = bad plans

**EXPLAIN PLAN — read it first:**
```sql
-- Snowflake: Query Profile (UI) or EXPLAIN
EXPLAIN SELECT p.payer_name, SUM(f.billed_amount)
FROM fact_claims f JOIN dim_payer p ON f.payer_sk = p.payer_sk
WHERE f.claim_date >= '2024-01-01'
GROUP BY 1;

-- Spark: explain()
df.explain(mode="extended")   # logical + physical plan
df.explain(mode="cost")       # with estimated row counts

-- BigQuery: Query plan in UI → Execution details
-- Look for: input rows vs output rows ratio at each stage
-- High input / low output = filter or join is selective (good)
-- Low input / high output = exploding join (bad)
```

**Key optimization techniques:**

**1. Filter pushdown — filter as early as possible**
```sql
-- ❌ Filter after join (scans entire fact table first)
SELECT p.name, f.amount
FROM fact_claims f JOIN dim_patient p ON f.patient_sk = p.patient_sk
WHERE p.state = 'CA';

-- ✅ Subquery pushes filter to dim scan first
SELECT p.name, f.amount
FROM fact_claims f
JOIN (SELECT patient_sk, name FROM dim_patient WHERE state = 'CA') p
ON f.patient_sk = p.patient_sk;
-- Modern optimizers often do this automatically — but not always
```

**2. Avoid SELECT * — column pruning**
```sql
-- ❌ Reads all columns from columnar store
SELECT * FROM fact_claims WHERE claim_date = '2024-01-15';

-- ✅ Only reads needed columns from disk (columnar store advantage)
SELECT claim_id, billed_amount, status FROM fact_claims
WHERE claim_date = '2024-01-15';
```

**3. Avoid functions on indexed/clustered columns in WHERE**
```sql
-- ❌ Function on column prevents pruning
WHERE YEAR(claim_date) = 2024

-- ✅ Range predicate enables micro-partition / partition pruning
WHERE claim_date BETWEEN '2024-01-01' AND '2024-12-31'
```

**4. Avoid DISTINCT as a crutch**
```sql
-- ❌ DISTINCT to hide upstream join bug
SELECT DISTINCT patient_id FROM fact_claims f JOIN dim_patient p ...

-- ✅ Fix the JOIN cardinality (use EXISTS or fix grain)
SELECT patient_id FROM dim_patient
WHERE EXISTS (SELECT 1 FROM fact_claims WHERE patient_sk = dim_patient.patient_sk)
```

### Production Example — Snowflake Query Tuning
```sql
-- Diagnose: Query Profile shows "Bytes spilled to remote storage"
-- = warehouse memory overflow → query spilling to S3 (slow)

-- Fix 1: Scale up warehouse (more memory per node)
ALTER WAREHOUSE analytics_wh SET WAREHOUSE_SIZE = 'LARGE';

-- Fix 2: Rewrite to reduce intermediate result size
-- Before: cartesian-prone pattern
SELECT * FROM a JOIN b ON a.key = b.key JOIN c ON b.key2 = c.key2;

-- After: filter early, join smaller sets
WITH filtered_a AS (SELECT key, col1 FROM a WHERE col1 IS NOT NULL),
     filtered_b AS (SELECT key, key2 FROM b WHERE status = 'ACTIVE')
SELECT fa.col1, c.col3
FROM filtered_a fa
JOIN filtered_b fb ON fa.key = fb.key
JOIN c ON fb.key2 = c.key2;

-- Fix 3: Add query result cache hint (force bypass for debugging)
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
```

### Common Mistakes
- ❌ Not collecting statistics before running CBO queries — optimizer makes wrong plan
- ❌ `SELECT *` on wide tables in columnar stores — reads all columns unnecessarily
- ❌ Implicit type conversion in WHERE (`WHERE int_col = '123'`) — prevents index/stat use
- ❌ Trusting the optimizer blindly — always check EXPLAIN for large queries
- ✅ Run `ANALYZE TABLE` (Spark/Hive) or `UPDATE STATISTICS` after large loads
- ✅ Snowflake: check Query Profile → "Partitions scanned" vs "Partitions total" → pruning effectiveness

### Interview Questions
1. What is the difference between rule-based and cost-based optimization?
2. Why does applying a function to a WHERE clause column prevent partition pruning?
3. How do you read a Spark execution plan and identify bottlenecks?
4. What causes query spilling to disk in Snowflake and how do you fix it?
5. Why is `SELECT *` expensive in columnar stores specifically?

---

## 9.2 Join Strategies

### Theory

**Three fundamental join algorithms:**

**1. Nested Loop Join**
```
For each row in table A:
    For each row in table B:
        if join condition: emit row
Complexity: O(N × M)
Use: small tables, no equi-join condition possible
Avoid: large tables — quadratic cost
```

**2. Hash Join (most common in data warehousing)**
```
Build phase:  Load smaller table into hash table (key → row)
Probe phase:  Scan larger table, probe hash table by join key
Complexity:   O(N + M)
Use:          Equi-joins, large tables
Requirement:  Hash table (smaller side) must fit in memory
              → OOM = spill to disk = slow
```

**3. Sort-Merge Join**
```
Sort both tables by join key
Merge sorted outputs (like merge sort)
Complexity: O(N log N + M log M)
Use:        Pre-sorted data, range joins, inequality joins
Advantage:  No hash table in memory requirement
```

**Broadcast Join (small table optimization):**
```
Broadcast small table to ALL worker nodes.
Each worker does local hash join — no shuffle of large table.
Spark threshold: spark.sql.autoBroadcastJoinThreshold (default 10MB)
BigQuery: automatic for small tables
Snowflake: automatic optimization
```

```python
# Spark: force broadcast join
from pyspark.sql.functions import broadcast

result = large_fact_df.join(
    broadcast(small_dim_df),   # broadcast dim to all executors
    on="patient_sk",
    how="inner"
)

# Or hint in SQL
spark.sql("""
    SELECT /*+ BROADCAST(p) */ f.*, p.payer_name
    FROM fact_claims f
    JOIN dim_payer p ON f.payer_sk = p.payer_sk
""")
```

**Shuffle (Sort-Merge) Join — default for large tables:**
```
Both tables partitioned by join key → sent to same reducer
Each reducer does local join
Cost: network shuffle of BOTH tables = expensive
Optimization: pre-partition both tables by join key (bucket join)
```

**Skew Join — handling data skew:**
```
Problem: one join key value has 80% of rows → one task runs for hours
         while all others finish in seconds

Solutions:
1. Salting: add random suffix to hot key, replicate dimension
   hot_key → hot_key_0, hot_key_1, ..., hot_key_N (distribute load)

2. Spark AQE (Adaptive Query Execution):
   spark.sql.adaptive.enabled = true
   → auto-detects skew, splits skewed partitions at runtime

3. Skew hint:
   SELECT /*+ SKEW('f', 'patient_sk') */ ...
```

```python
# Spark AQE — handles skew automatically
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
```

### Tradeoffs
| Join Type | Memory | CPU | Network | Best For |
|---|---|---|---|---|
| Nested Loop | Low | Very High | Low | Tiny tables, inequality |
| Hash Join | High (hash table) | Medium | High (shuffle) | Large equi-joins |
| Sort-Merge | Medium | High (sort) | High (shuffle) | Pre-sorted, ranges |
| Broadcast | Low (small side) | Low | Low | One small table |
| Bucket Join | Low | Low | Low | Pre-bucketed tables |

### Common Mistakes
- ❌ Joining on non-equi conditions without awareness — forces nested loop or sort-merge
- ❌ Broadcast join with a "small" table that is actually 500MB — OOM on executors
- ❌ Not enabling AQE in Spark — missing automatic skew handling and partition coalescing
- ❌ Joining before filtering — join result is larger than necessary
- ✅ Always filter BEFORE joining — reduce both sides as much as possible first
- ✅ Join order: largest table last in Spark SQL (right side = probe side in hash join)

### Interview Questions
1. Explain the difference between hash join and sort-merge join.
2. When would you use a broadcast join and what are its risks?
3. What is data skew in a join and how do you detect and fix it in Spark?
4. What is Adaptive Query Execution (AQE) in Spark and what problems does it solve?
5. Why is join order important and how does the optimizer determine it?

---

## 9.3 Indexing

### Theory

Indexes trade write overhead and storage for faster read access. Strategy differs by system.

**B-Tree Index (OLTP standard):**
- Balanced tree structure. O(log N) lookup.
- Best for: equality and range queries on high-cardinality columns.
- Bad for: low-cardinality columns (2 values = full scan faster).
- Used in: PostgreSQL, MySQL, Oracle.

**Bitmap Index (OLAP / DWH):**
- One bit per row per distinct value. Compact for low-cardinality columns.
- Fast for AND/OR operations across multiple bitmap indexes.
- Bad for: high-cardinality, high-update tables (bitmap rebuild costly).
- Used in: Oracle DWH, older DWH systems.

**Columnar Store "Indexes" (modern DWH — not traditional indexes):**

| System | Mechanism | How it works |
|---|---|---|
| Snowflake | Micro-partition metadata | Min/max/null stats per column per MP |
| Snowflake | Search Optimization Service | Point-lookup acceleration index |
| BigQuery | Clustering | Column-sorted within storage blocks |
| Delta Lake | Z-order | Multi-dimensional locality |
| Delta Lake | Bloom filter index | Probabilistic membership per file |
| Spark | Bucketing | Pre-partition by join/group key |

**Bloom Filter Index (Delta Lake):**
```sql
-- Create bloom filter on high-cardinality lookup column
CREATE BLOOMFILTER INDEX ON TABLE claims
FOR COLUMNS(claim_id OPTIONS (fpp=0.1));  -- 10% false positive rate

-- Query: "WHERE claim_id = 'CLM-2024-001'"
-- Bloom filter: check each file → is claim_id possibly in this file?
-- If NO → skip file entirely (no false negatives)
-- If YES → read file and check (may have false positives = false positive rate)
```

**Snowflake Search Optimization Service:**
```sql
-- Accelerate point lookup queries (equality predicates)
ALTER TABLE dim_patient ADD SEARCH OPTIMIZATION
ON EQUALITY(patient_nk, ssn, email);

-- Cost: additional storage + maintenance credits
-- Benefit: point lookups go from seconds → milliseconds
-- Best for: claim lookup by ID, patient lookup by SSN
```

### Common Mistakes
- ❌ Creating indexes on every column in OLTP — too many indexes = slow writes
- ❌ B-tree index on low-cardinality column (gender: M/F) — full scan is faster
- ❌ Forgetting to update statistics after adding index — optimizer doesn't use it
- ❌ Search Optimization on all columns in Snowflake — cost exceeds benefit
- ✅ Index only columns that appear in WHERE, JOIN ON, or ORDER BY with high selectivity

### Interview Questions
1. Why are bitmap indexes suited for OLAP but not OLTP?
2. How does a bloom filter index work and what is a false positive rate?
3. What is Snowflake's Search Optimization Service and when should you enable it?
4. Why does adding too many indexes hurt OLTP write performance?

---

## 9.4 Predicate Pushdown

### Theory

**Predicate pushdown:** Move filter conditions as close to the data source as possible — minimize data read before filtering occurs.

**Levels of pushdown:**

**1. Partition pruning** — skip entire partitions
```
WHERE claim_date = '2024-01-15'
→ Only read partition year=2024/month=01/day=15/
→ Skip all other partitions (zero I/O)
```

**2. File/micro-partition pruning** — skip entire files
```
WHERE payer_id = 'PAY-001'
→ Parquet: check row group stats (min/max payer_id per row group)
→ Skip row groups where max(payer_id) < 'PAY-001' OR min > 'PAY-001'
→ Snowflake: skip micro-partitions where payer_id range doesn't overlap
```

**3. Row group / stripe pruning** — skip within file
```
→ Parquet: skip row groups that don't match stats
→ ORC: skip stripes using bloom filters
```

**4. Source pushdown (connector-level)**
```python
# Spark reading from JDBC: push WHERE to database
df = spark.read.jdbc(
    url=jdbc_url,
    table="claims",
    predicates=["claim_date >= '2024-01-01' AND status = 'APPROVED'"],
    # ^ sent as SQL WHERE to source DB — only matching rows transferred
    properties={"driver": "oracle.jdbc.driver.OracleDriver"}
)

# vs full scan + filter in Spark (bad):
df = spark.read.jdbc(url, "claims", properties=props)  # reads everything
    .filter("claim_date >= '2024-01-01'")               # filters in Spark memory
```

**Pushdown limits — when it DOESN'T work:**
```
1. Function on column: WHERE YEAR(claim_date) = 2024
   → Optimizer can't use stats (result of function unknown at plan time)
   → Fix: WHERE claim_date BETWEEN '2024-01-01' AND '2024-12-31'

2. OR across different columns: WHERE col_a = 'X' OR col_b = 'Y'
   → Can't prune on both simultaneously
   → Fix: UNION ALL of two separate filtered queries

3. Non-deterministic functions: WHERE col > RAND()
   → Stats can't predict which rows match

4. CAST/implicit conversion: WHERE int_col = '123'
   → Type mismatch prevents stat comparison
```

### Common Mistakes
- ❌ `WHERE DATE_TRUNC('month', claim_date) = '2024-01-01'` — function prevents pushdown
- ❌ OR conditions across multiple columns — neither column pruned
- ❌ Implicit type casting in predicates — silently disables stat-based pruning
- ✅ Use range predicates (`BETWEEN`) instead of date functions
- ✅ Test with EXPLAIN: check "Partitions scanned" before/after predicate rewrite

### Interview Questions
1. What is predicate pushdown and at what levels can it occur?
2. Why does applying a SQL function to a filter column break predicate pushdown?
3. How does Spark push predicates down to a JDBC source?
4. What is the difference between partition pruning and row group pruning in Parquet?

---

## 9.5 Caching

### Theory

| Cache Level | Where | What | Lifetime | Cost |
|---|---|---|---|---|
| Snowflake Result Cache | Cloud Services | Exact query result | 24h | Free |
| Snowflake Local Cache | VW SSD | Column data | VW session | Included |
| Spark `.cache()` | Executor memory/disk | DataFrame | Job session | Memory |
| BigQuery BI Engine | In-memory | Table slices | Session | BI Engine slots |
| Redis | External | Aggregated results | TTL | Infra cost |
| CDN | Edge | Dashboard snapshots | TTL | CDN cost |

**Spark caching strategy:**
```python
# Cache DataFrame used multiple times
dim_patient_df = spark.read.parquet("s3://datalake/silver/dim_patient/")
dim_patient_df.cache()          # lazy — cached on first action
dim_patient_df.count()          # trigger cache population

# Check if cached
print(spark.catalog.isCached("dim_patient"))

# Persist with storage level
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # serialized = less memory, slower

# Always unpersist when done
dim_patient_df.unpersist()

# When to cache:
# ✅ DataFrame used in 3+ downstream actions
# ✅ Expensive to recompute (complex joins, large aggregations)
# ❌ DataFrame used only once — caching adds overhead
# ❌ DataFrame larger than available executor memory — spills to disk, may be slower
```

### Common Mistakes
- ❌ Caching every DataFrame "just in case" — wastes memory, may evict more useful cached data
- ❌ Not unpersisting cached DataFrames — executor memory fills up, GC pressure
- ❌ Caching after a shuffle — the shuffle result is the expensive part; cache before actions that trigger re-shuffle
- ✅ Cache DataFrames that are reused in iterative algorithms (ML training loops)
- ✅ Snowflake: don't suspend VW between related queries — lose local disk cache

### Interview Questions
1. What is the difference between Snowflake's result cache and local disk cache?
2. When should you NOT cache a Spark DataFrame?
3. What is the difference between `cache()` and `persist()` in Spark?
4. How does BigQuery BI Engine differ from standard BigQuery caching?

---

## 9.6 Statistics

### Theory

Statistics are metadata describing data distribution. CBO uses them to estimate plan costs.

**Key statistics:**
- **Row count:** Number of rows in table
- **Column cardinality:** Number of distinct values per column
- **Histogram:** Distribution of values (frequency per bucket)
- **Null count:** Number of nulls per column
- **Min / Max:** Value range per column

**Collecting statistics:**
```sql
-- Spark: analyze table
ANALYZE TABLE claims COMPUTE STATISTICS;
ANALYZE TABLE claims COMPUTE STATISTICS FOR COLUMNS claim_date, payer_id, amount;

-- Hive
ANALYZE TABLE claims COMPUTE STATISTICS;
ANALYZE TABLE claims COMPUTE STATISTICS FOR COLUMNS;

-- Snowflake: automatic (maintained continuously by cloud services layer)
-- No manual ANALYZE needed — stats always current

-- BigQuery: automatic (maintained by Capacitor storage layer)
-- INFORMATION_SCHEMA.TABLE_STATISTICS for inspection

-- PostgreSQL
ANALYZE claims;                    -- update statistics
VACUUM ANALYZE claims;             -- reclaim space + update stats
```

**Stale statistics = bad query plans:**
```
Table had 1M rows when last analyzed.
Now has 500M rows after 6 months of growth.
Optimizer thinks it's 1M → chooses broadcast join (wrong for 500M rows)
→ OOM on executor
Fix: re-analyze table after significant data growth
```

### Common Mistakes
- ❌ Loading massive data without re-running ANALYZE — optimizer uses stale stats
- ❌ Not collecting column-level statistics — CBO can't estimate join cardinality
- ❌ Assuming Snowflake/BigQuery never have stale stats — check after bulk loads
- ✅ Schedule ANALYZE as part of post-load pipeline step

### Interview Questions
1. What statistics does a CBO use to estimate query cost?
2. What happens when statistics are stale in a cost-based optimizer?
3. Why does Snowflake not require manual ANALYZE?

---

## 9.7 Cost Optimization

### Theory

**Snowflake cost levers:**
```sql
-- 1. Auto-suspend idle warehouses (biggest single saving)
ALTER WAREHOUSE wh SET AUTO_SUSPEND = 60;  -- 60 seconds

-- 2. Right-size warehouses (common over-provisioning)
-- Run query on XS → check spillage → scale up only if needed
-- Scale OUT (multi-cluster) only for concurrency, not single query

-- 3. Resource monitor: hard cap
CREATE RESOURCE MONITOR monthly_cap
    WITH CREDIT_QUOTA = 1000           -- monthly credits
    TRIGGERS ON 90 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- 4. Query tags for cost attribution
ALTER SESSION SET QUERY_TAG = 'team=analytics,pipeline=claims_daily';
-- Query history → GROUP BY query_tag → team-level cost

-- 5. Storage optimizer: remove old data
ALTER TABLE fact_claims SET DATA_RETENTION_TIME_IN_DAYS = 7;  -- reduce from 90
-- Each day of time travel = additional storage cost

-- 6. Materialized views for repeated expensive queries
CREATE MATERIALIZED VIEW mv_monthly_claims AS
SELECT payer_id, DATE_TRUNC('month', claim_date) AS month,
       SUM(billed_amount) AS total_billed, COUNT(*) AS claim_count
FROM fact_claims
GROUP BY 1, 2;
-- MV auto-refreshed. Query hits MV not base table → faster + less compute.
```

**BigQuery cost levers:**
```sql
-- 1. Partition filtering (REQUIRED for cost control)
-- require_partition_filter = TRUE forces WHERE on partition column
-- Prevents accidental full-table scans

-- 2. Column selection (charged by bytes scanned)
SELECT claim_id, amount FROM fact_claims   -- cheap
-- vs
SELECT * FROM fact_claims                  -- expensive

-- 3. Materialized views (BQ computes incrementally)
CREATE MATERIALIZED VIEW mv_payer_monthly AS
SELECT payer_id, EXTRACT(YEAR FROM claim_date) yr,
       SUM(billed_amount) total
FROM fact_claims GROUP BY 1, 2;

-- 4. Slot reservations for predictable high-volume workloads
-- Flat-rate: $2,000/month for 100 slots
-- vs On-demand: $6.25/TB → if >320TB/month → flat-rate wins

-- 5. Table expiration for transient data
CREATE TABLE temp_analysis
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY));
```

**Spark cost levers (EMR / Databricks):**
```python
# 1. Spot/Preemptible instances for worker nodes (60-70% savings)
# Keep on-demand for driver only

# 2. Auto-scaling clusters
# scale down when idle, scale up on demand

# 3. Avoid re-computation: cache strategically
# 4. Coalesce before writing to avoid small files (= fewer S3 API calls)
df.coalesce(100).write.parquet("s3://...")

# 5. Use columnar formats (Parquet) not CSV — less S3 data transfer
# 6. Use Delta OPTIMIZE to reduce file count (fewer LIST + GET API calls)
```

### Interview Questions
1. What is the single biggest Snowflake cost-saving action most teams overlook?
2. How do you implement team-level cost attribution in Snowflake?
3. When does BigQuery flat-rate pricing beat on-demand pricing?
4. How do materialized views reduce compute cost in Snowflake and BigQuery?
5. Why do small files increase S3/cloud storage costs beyond just storage?

---

*Next: Chunk 10 — Governance & Security*
# DE Master Doc — Chunk 10: Governance & Security
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 10.1 RBAC vs ABAC

### Theory

**RBAC (Role-Based Access Control):**
Access determined by the roles assigned to a user. User → Role → Permissions.
Simple, auditable, scalable for most enterprise use cases.

```sql
-- Snowflake RBAC example
CREATE ROLE claims_analyst;
CREATE ROLE claims_engineer;
CREATE ROLE pii_viewer;

-- Grant object privileges to role
GRANT SELECT ON DATABASE claims_dw TO ROLE claims_analyst;
GRANT SELECT, INSERT, UPDATE ON SCHEMA claims_dw.gold TO ROLE claims_engineer;
GRANT SELECT ON TABLE dim_patient TO ROLE pii_viewer;

-- Assign role to user
GRANT ROLE claims_analyst TO USER john_doe;
GRANT ROLE pii_viewer TO ROLE claims_analyst;  -- role hierarchy

-- Role hierarchy: claims_engineer inherits all claims_analyst permissions
GRANT ROLE claims_analyst TO ROLE claims_engineer;
```

**ABAC (Attribute-Based Access Control):**
Access determined by attributes of the user, resource, and environment.
Fine-grained. Policy-driven. More complex but more flexible.

```
Policy example:
  ALLOW access IF:
    user.department = 'Claims'
    AND user.clearance_level >= resource.sensitivity_level
    AND environment.time BETWEEN '08:00' AND '18:00'
    AND resource.data_region = user.assigned_region

Implementations:
  - Snowflake: Row Access Policies (ABAC-style at row level)
  - AWS Lake Formation: tag-based access control
  - Apache Ranger: policy engine for Hadoop ecosystem
  - OPA (Open Policy Agent): general-purpose policy engine
```

**RBAC vs ABAC:**
| Dimension | RBAC | ABAC |
|---|---|---|
| Complexity | Low | High |
| Granularity | Role-level | Attribute-level (row/column/time) |
| Flexibility | Medium | Very High |
| Auditability | Simple | Complex |
| Best for | Enterprise standard access | Fine-grained dynamic access |
| Snowflake implementation | Roles + grants | Row Access Policies + masking |

### Production Example — Snowflake RBAC + ABAC
```sql
-- RBAC: standard role-based access
CREATE ROLE payer_analyst_bcbs;
GRANT USAGE ON DATABASE claims_dw TO ROLE payer_analyst_bcbs;
GRANT SELECT ON ALL TABLES IN SCHEMA claims_dw.gold TO ROLE payer_analyst_bcbs;

-- ABAC: row-level access by payer (Row Access Policy)
CREATE ROW ACCESS POLICY rap_payer_filter
AS (payer_id VARCHAR) RETURNS BOOLEAN ->
    payer_id IN (
        SELECT payer_id FROM access_control.user_payer_map
        WHERE snowflake_user = CURRENT_USER()
    );

ALTER TABLE fact_claims ADD ROW ACCESS POLICY rap_payer_filter ON (payer_id);
-- User john_doe (BCBS analyst) sees ONLY BCBS rows in fact_claims
-- Policy evaluated at query time — zero code change needed

-- Column masking (ABAC on column sensitivity)
CREATE MASKING POLICY mp_ssn_mask
AS (val STRING) RETURNS STRING ->
    CASE
        WHEN IS_ROLE_IN_SESSION('PII_VIEWER') THEN val       -- see full SSN
        ELSE CONCAT('***-**-', RIGHT(val, 4))                -- masked
    END;

ALTER TABLE dim_patient MODIFY COLUMN ssn SET MASKING POLICY mp_ssn_mask;
```

### Common Mistakes
- ❌ Role explosion: one role per user → defeats purpose of RBAC (50 users = 50 roles)
- ❌ GRANT ALL to DATA_ENGINEER role — engineers don't need DDL on prod
- ❌ Bypassing row access policies by querying raw/bronze layer — policies must cover ALL layers
- ❌ Hard-coding user names in access policies — use dynamic functions (CURRENT_USER())
- ✅ Minimum privilege principle: grant only what's needed, revoke when role changes
- ✅ Audit GRANT history regularly: `SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES`

### Interview Questions
1. What is the difference between RBAC and ABAC?
2. How does Snowflake implement row-level security?
3. How would you implement payer-specific data isolation in a multi-tenant DWH?
4. What is the principle of least privilege and how do you enforce it in Snowflake?
5. How does AWS Lake Formation implement tag-based access control?

---

## 10.2 Row and Column Security

### Theory

**Column-Level Security:**
Restrict which columns a user/role can see. Applied via masking (show transformed value) or restriction (error on access).

**Row-Level Security (RLS):**
Restrict which rows a user/role can see. Applied via row access policies evaluated at query time.

**Implementation across platforms:**
```sql
-- Snowflake: Dynamic Data Masking (column)
CREATE MASKING POLICY email_mask AS (val STRING) RETURNS STRING ->
    CASE
        WHEN CURRENT_ROLE() = 'PII_ANALYST' THEN val
        WHEN CURRENT_ROLE() = 'MARKETING' THEN SHA2(val)   -- hash for analytics
        ELSE '****@****.***'                                 -- fully masked
    END;

-- Snowflake: Row Access Policy (row)
CREATE ROW ACCESS POLICY region_filter AS (region CHAR(2)) RETURNS BOOLEAN ->
    region IN (
        SELECT allowed_region FROM user_region_map
        WHERE username = CURRENT_USER()
    );

-- BigQuery: Column-level access
-- Via authorized views or column-level policy tags
-- Policy tag: "SENSITIVE_PII" → only tagged-role users can query

-- Databricks Unity Catalog: Column masking
CREATE FUNCTION mask_ssn(val STRING)
    RETURN CASE WHEN is_member('phi_viewers') THEN val
                ELSE CONCAT('***-**-', RIGHT(val, 4)) END;

ALTER TABLE dim_patient ALTER COLUMN ssn
    SET MASK mask_ssn USING COLUMNS (ssn);

-- dbt: column-level tags for governance
-- models/schema.yml
columns:
  - name: ssn
    tags: [pii, hipaa]
    meta:
      masking_policy: mp_ssn_mask
```

### Common Mistakes
- ❌ Security enforced only at BI layer (Power BI RLS) — bypassed by direct SQL access
- ❌ Masking applied to column but raw table accessible directly — mask the source, not just the view
- ❌ Row Access Policy with expensive subquery — evaluated per query; slow policy = slow queries
- ✅ Security must be at the storage/DWH layer, not only the presentation layer
- ✅ Test policies as restricted users: `EXECUTE AS USER = 'john_doe'` (Snowflake)

### Interview Questions
1. What is the difference between masking and restricting column access?
2. Why is applying security only at the BI layer insufficient?
3. How do you test that row access policies are working correctly in Snowflake?
4. What is a policy tag in BigQuery and how does it enforce column-level security?

---

## 10.3 Encryption

### Theory

**Encryption at rest:**
Data encrypted on disk/storage. Protects against physical theft of storage media.

| Platform | Mechanism | Key Management |
|---|---|---|
| S3 | SSE-S3 (AWS-managed) or SSE-KMS | AWS KMS or customer-managed |
| ADLS Gen2 | AES-256 | Azure Key Vault |
| GCS | AES-256 (default) | Cloud KMS |
| Snowflake | AES-256 (Tri-Secret Secure) | Snowflake + customer key |
| BigQuery | AES-256 (default) or CMEK | Cloud KMS |

**Encryption in transit:**
Data encrypted while moving over network. TLS 1.2/1.3 minimum.
- S3 → enforce HTTPS-only via bucket policy
- Kafka → TLS between brokers and clients
- JDBC → `ssl=true` in connection string
- Snowflake → TLS enforced on all connections

**Encryption in use (emerging):**
Data encrypted while being processed. Prevents cloud provider from seeing plaintext.
- Snowflake Tri-Secret Secure: customer holds one of three keys; Snowflake cannot decrypt without customer key
- Confidential computing: Intel SGX / AMD SEV (encrypted in CPU memory)

**Key rotation:**
```python
# AWS KMS: automatic key rotation (annual)
aws kms enable-key-rotation --key-id <key-id>

# Snowflake: periodic re-encryption on key rotation (Enterprise)
# Key rotation configured by Snowflake automatically
# Customer master key rotation = immediate re-encryption of all data keys
```

**Tokenization vs Encryption:**
- **Encryption:** Transform plaintext → ciphertext using key. Reversible with key.
- **Tokenization:** Replace sensitive value with random token. Token ↔ value mapping stored in vault. No mathematical relationship. Better for PCI-DSS (credit card numbers).

### HIPAA / GDPR Encryption Requirements
```
HIPAA (Healthcare):
  • PHI must be encrypted at rest and in transit
  • AES-128 minimum (AES-256 recommended)
  • Key management audit trail required

GDPR (Europe):
  • Pseudonymization recommended (not required if encrypted)
  • Right to erasure: encryption key deletion = effective deletion
  • Key deletion ≠ data deletion physically, but renders data unreadable

PCI-DSS (Payment Card):
  • Strong cryptography for cardholder data
  • Tokenization preferred over encryption for storage
  • Key management policy required (rotation, split knowledge)
```

### Common Mistakes
- ❌ Encrypting data but storing encryption key alongside it (same S3 bucket)
- ❌ TLS 1.0/1.1 in transit — vulnerable, deprecated; enforce TLS 1.2+
- ❌ Not rotating keys — long-lived keys increase exposure window
- ❌ Thinking encryption eliminates compliance requirements — encryption is necessary but not sufficient
- ✅ Separate key management from data storage (KMS as separate service)
- ✅ Audit key access: who decrypted what, when (CloudTrail / Snowflake ACCOUNT_USAGE)

### Interview Questions
1. What is the difference between encryption at rest, in transit, and in use?
2. What is Snowflake Tri-Secret Secure and what compliance scenario requires it?
3. How does key deletion achieve "right to erasure" under GDPR?
4. What is the difference between tokenization and encryption for PCI-DSS compliance?

---

## 10.4 Data Lineage

### Theory

**Data lineage:** Tracks the origin, movement, transformation, and consumption of data across its lifecycle.

**Why lineage matters:**
- **Impact analysis:** If dim_patient changes schema, which downstream tables break?
- **Root cause analysis:** Why does this dashboard show wrong numbers? Trace back to source.
- **Compliance:** GDPR/HIPAA require knowing where PII is used.
- **Trust:** Business users trust data more when they can see its origin.

**Lineage granularity levels:**
```
Table-level:  fact_claims ← silver_claims ← bronze_claims ← Oracle EHR
Column-level: fact_claims.billed_amount ← silver_claims.claim_amount ← oracle.CLM_AMOUNT
Job-level:    Airflow DAG → Spark job → dbt model → Power BI dataset
```

**Lineage tools:**
| Tool | Lineage Type | Integration |
|---|---|---|
| Apache Atlas | Table + column | Hive, Spark, Kafka, HBase |
| Microsoft Purview | Table + column + BI | Azure, Power BI, SQL Server |
| Collibra DGC | Business + technical | All major platforms via connectors |
| DataHub (LinkedIn) | Table + column + job | Kafka, Spark, dbt, Airflow |
| dbt | Column-level (SQL parse) | dbt models only |
| OpenLineage | Standard API | Airflow, Spark, dbt, Flink |
| Snowflake Access History | Column-level | Snowflake native |

**OpenLineage standard:**
```json
{
  "eventType": "COMPLETE",
  "job": {
    "namespace": "airflow",
    "name": "claims_pipeline.spark_silver_transform"
  },
  "inputs": [{
    "namespace": "s3://datalake",
    "name": "bronze/claims",
    "facets": {"schema": {"fields": [{"name": "claim_id"}, {"name": "amount"}]}}
  }],
  "outputs": [{
    "namespace": "s3://datalake",
    "name": "silver/claims",
    "facets": {"columnLineage": {
      "fields": {
        "claim_amount": {"inputFields": [{"name": "amount", "field": "amount"}]}
      }
    }}
  }]
}
```

**dbt lineage (automatic from SQL parsing):**
```yaml
# dbt auto-generates DAG from ref() calls
# fact_claims references dim_patient, dim_payer, stg_claims
# dbt docs → visual lineage graph in browser

# Column-level lineage via dbt-osmosis or dbt 1.6+ column lineage
```

**Snowflake native lineage:**
```sql
-- Access history: which columns were read by which query
SELECT query_id, user_name, direct_objects_accessed, base_objects_accessed
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE query_start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
AND ARRAY_SIZE(direct_objects_accessed) > 0
ORDER BY query_start_time DESC;
```

### Common Mistakes
- ❌ Lineage as afterthought — retrofitting lineage to existing systems is very expensive
- ❌ Table-level only lineage — insufficient for GDPR column-level PII tracking
- ❌ Lineage tool disconnected from actual pipelines — lineage must be auto-captured, not manually documented
- ✅ Use OpenLineage standard from the start — works with Airflow, Spark, dbt, Flink natively
- ✅ Collibra/Purview: business lineage (business term → technical column → report) for compliance

### Interview Questions
1. What is data lineage and why is column-level lineage important for GDPR compliance?
2. How does dbt generate lineage automatically?
3. What is the OpenLineage standard and why is it important for multi-tool pipelines?
4. How do you use lineage for impact analysis when a source schema changes?
5. How does Snowflake's Access History differ from external lineage tools?

---

## 10.5 Metadata Management

### Theory

**Types of metadata:**

| Type | Examples | Tools |
|---|---|---|
| Technical metadata | Schema, data types, row counts, partitions, file size | Glue Catalog, Hive Metastore, Unity Catalog |
| Business metadata | Business definitions, owners, stewards, SLAs | Collibra, Purview, DataHub |
| Operational metadata | Job run history, latency, error rates, freshness | Airflow, DataHub, Monte Carlo |
| Social metadata | Usage stats, ratings, certifications, comments | DataHub, Atlan |
| Lineage metadata | Origin, transformations, downstream dependencies | Atlas, Purview, OpenLineage |

**Data Catalog (the central hub):**
A searchable, governed inventory of all data assets. Business users find, understand, and trust data before using it.

**Collibra DGC (Data Governance Center):**
```
Key components:
  Business Glossary: Canonical definition of business terms
    "Claim" → definition, owner, steward, related terms
  Data Dictionary: Technical metadata (tables, columns, types)
  Data Lineage: Source → transformation → consumption
  Policy Manager: GDPR/HIPAA policy documentation + enforcement
  Workflow: Certification, issue resolution, change management
  Data Quality: DQ scores per asset

Integration with Snowflake:
  Collibra Catalog → Snowflake connector → auto-harvest:
  • Table/column metadata
  • Access history (who uses what)
  • Data quality metrics
  → Business glossary terms linked to physical columns
```

**Unity Catalog (Databricks):**
```python
# Three-level namespace: catalog.schema.table
spark.sql("CREATE CATALOG prod_catalog")
spark.sql("CREATE SCHEMA prod_catalog.claims")
spark.sql("CREATE TABLE prod_catalog.claims.fact_claims ...")

# Tag columns for governance
spark.sql("""
    ALTER TABLE prod_catalog.claims.fact_claims
    ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'sensitivity' = 'high')
""")

# Column-level lineage automatic via Unity Catalog
# System tables: system.access.audit, system.lineage.table_lineage
```

### Common Mistakes
- ❌ Data catalog populated manually — becomes stale immediately; must auto-harvest
- ❌ Technical catalog without business glossary — engineers know what a column is, business doesn't trust it
- ❌ No data stewardship model — catalog without owners = orphaned, untrustworthy metadata
- ✅ Auto-harvest technical metadata; business team enriches with definitions, classifications
- ✅ Link business glossary terms to physical columns (Collibra) — bridge business and technical

### Interview Questions
1. What is the difference between a data dictionary and a data catalog?
2. What are the five types of metadata and why does each matter?
3. How does Collibra DGC integrate with Snowflake?
4. What is Unity Catalog in Databricks and how does it differ from Hive Metastore?

---

## 10.6 Data Quality Frameworks

### Theory

**Data quality dimensions:**

| Dimension | Definition | Example Failure |
|---|---|---|
| Completeness | Required fields populated | patient_id IS NULL |
| Accuracy | Values correct / match source | amount = -500 (invalid) |
| Consistency | Same entity same value across systems | Patient DOB differs in EHR vs claims |
| Timeliness | Data available when needed | Yesterday's claims not loaded by 6 AM |
| Uniqueness | No unexpected duplicates | claim_id appears twice |
| Validity | Values within allowed domain | status_code = 'XYZ' (not in allowed list) |
| Referential integrity | FK exists in dim table | payer_sk not in dim_payer |

**DQ tools:**

**Great Expectations:**
```python
import great_expectations as gx

context = gx.get_context()

# Define expectations
suite = context.create_expectation_suite("claims_silver_suite")

validator = context.get_validator(
    datasource_name="s3_claims",
    data_asset_name="silver_claims",
    expectation_suite_name="claims_silver_suite"
)

# Completeness
validator.expect_column_values_to_not_be_null("claim_id")
validator.expect_column_values_to_not_be_null("patient_id")

# Uniqueness
validator.expect_column_values_to_be_unique("claim_id")

# Validity
validator.expect_column_values_to_be_in_set(
    "status_code", ["SUBMITTED", "APPROVED", "DENIED", "PENDING"]
)

# Accuracy / range
validator.expect_column_values_to_be_between(
    "billed_amount", min_value=0, max_value=1000000
)

# Referential integrity
validator.expect_column_values_to_be_in_set(
    "payer_id", value_set=valid_payer_ids
)

# Timeliness: freshness check
validator.expect_column_max_to_be_between(
    "claim_date",
    min_value=yesterday,
    max_value=today
)

validator.save_expectation_suite()

# Run checkpoint in Airflow pipeline
result = context.run_checkpoint("claims_daily_checkpoint")
if not result.success:
    raise ValueError("DQ validation failed — pipeline halted")
```

**dbt tests:**
```yaml
# models/schema.yml
models:
  - name: fact_claims
    columns:
      - name: claim_id
        tests:
          - unique
          - not_null
      - name: payer_sk
        tests:
          - relationships:
              to: ref('dim_payer')
              field: payer_sk
      - name: status_code
        tests:
          - accepted_values:
              values: ['SUBMITTED', 'APPROVED', 'DENIED', 'PENDING']
      - name: billed_amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 1000000
```

**Monte Carlo (Data Observability):**
- ML-based anomaly detection on table freshness, volume, schema changes, distribution shifts.
- No manual rule writing — learns normal patterns, alerts on deviations.
- Integrates with Snowflake, BigQuery, dbt, Airflow.
- Covers: table health score, lineage, incident management.

**DQ rule placement in pipeline:**
```
Bronze (ingest):  Schema validation only (tolerate source issues, don't block)
Bronze→Silver:    Full DQ suite (completeness, validity, uniqueness)
Silver→Gold:      Business rule validation (referential integrity, aggregation checks)
Gold (output):    Threshold checks (row count within ±5% of yesterday, sum reconciliation)
```

### Common Mistakes
- ❌ DQ only at the end of pipeline — bad data already propagated to Gold
- ❌ No DQ = pipeline runs but delivers wrong numbers silently
- ❌ Failing entire pipeline on first DQ error — use quarantine pattern (isolate bad rows, let good rows through)
- ❌ Static DQ rules without anomaly detection — doesn't catch gradual drift
- ✅ Quarantine pattern: route DQ-failing rows to error table, don't block pipeline
- ✅ DQ score trending: track DQ pass rate over time, alert on degradation

```python
# Quarantine pattern in Spark
good_rows = df.filter("claim_amount > 0 AND claim_id IS NOT NULL")
bad_rows  = df.filter("claim_amount <= 0 OR claim_id IS NULL")

good_rows.write.mode("append").save("s3://silver/claims/")
bad_rows.write.mode("append").save("s3://quarantine/claims/")
# Alert on quarantine table growth
```

### Interview Questions
1. What are the six data quality dimensions? Give an example failure for each.
2. What is the quarantine pattern and why is it better than failing the pipeline?
3. How does Great Expectations integrate into an Airflow pipeline?
4. What is data observability and how does it differ from data quality testing?
5. Where in the Medallion pipeline should DQ checks be applied?
6. How do dbt tests enforce referential integrity?

---

*Next: Chunk 11 — Production Problems*
# DE Master Doc — Chunk 11: Real Production Problems
> Format: Problem → Root Cause → Detection → Solution → Prevention → Interview Questions

---

## 11.1 Late-Arriving Data

### Problem
Data arrives after the expected processing window. A claim submitted on Jan 15 arrives in the pipeline on Jan 20. The Jan 15 batch has already run and closed.

### Root Causes
- Source system batch delay (hospital submits claims 3-5 days after service)
- Network/integration failures causing delayed delivery
- Manual data entry processes
- Resubmissions and corrections from upstream

### Detection
```sql
-- Detect late arrivals: ingestion_date >> event_date
SELECT
    COUNT(*) AS late_record_count,
    AVG(DATEDIFF('day', claim_date, ingestion_date)) AS avg_lateness_days,
    MAX(DATEDIFF('day', claim_date, ingestion_date)) AS max_lateness_days
FROM bronze_claims
WHERE DATEDIFF('day', claim_date, ingestion_date) > 1  -- arrived > 1 day late
GROUP BY DATE_TRUNC('month', claim_date);
```

### Solutions

**Solution 1: Reprocessing / Backfill**
Re-run pipeline for affected historical partitions when late data arrives.
Requires: idempotent pipeline (safe to re-run).
```python
# Airflow: trigger backfill for affected date range
airflow dags backfill claims_daily_pipeline \
    --start-date 2024-01-13 \
    --end-date 2024-01-15

# Delta Lake: overwrite specific partition with corrected data
df_late.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "claim_date BETWEEN '2024-01-13' AND '2024-01-15'") \
    .save("s3://silver/claims/")
```

**Solution 2: Watermarks (Streaming)**
In streaming pipelines, define maximum expected lateness. Events beyond watermark are dropped or sent to DLQ.
```python
# Flink / Spark Structured Streaming watermark
df_with_watermark = df_stream \
    .withWatermark("event_time", "5 days") \  # tolerate up to 5 days late
    .groupBy(
        window("event_time", "1 day"),         # 1-day tumbling window
        "payer_id"
    ).agg(sum("billed_amount"))
# Events more than 5 days late → dropped (or sent to late data topic)
```

**Solution 3: Accumulating Snapshot + Corrections**
Fact table designed to accept corrections. MERGE replaces outdated row.
```sql
-- Upsert late arrival into fact table
MERGE INTO fact_claims t
USING late_claims_staging s ON t.claim_nk = s.claim_nk
WHEN MATCHED AND s.ingestion_date > t.last_updated THEN
    UPDATE SET t.billed_amount = s.billed_amount,
               t.status = s.status,
               t.last_updated = s.ingestion_date
WHEN NOT MATCHED THEN INSERT VALUES (...);
```

**Solution 4: Audit columns**
Track both event time and ingestion time. Reports can filter by either.
```sql
ALTER TABLE fact_claims ADD COLUMN
    event_date       DATE,    -- when event occurred (business date)
    ingestion_date   DATE,    -- when data arrived in pipeline
    is_late_arrival  BOOLEAN  -- flag for transparency
;
-- Reports can show: "data as of event date" vs "data as loaded"
```

### Prevention
- SLA agreements with source systems
- Monitor ingestion lag: alert when ingestion_date - event_date > N days
- Design pipelines as idempotent from day 1

### Interview Questions
1. How do you handle late-arriving data in a batch pipeline?
2. What is a watermark in streaming and how does it handle late events?
3. Why must a pipeline be idempotent to support backfill for late data?
4. What is the difference between event time and processing time in streaming?

---

## 11.2 Duplicate Handling

### Problem
Same record appears multiple times in the pipeline. Causes: at-least-once delivery, retries, source system bugs, double-click by user.

### Detection
```sql
-- Detect duplicates: count > 1 per business key
SELECT claim_id, COUNT(*) AS cnt
FROM fact_claims
GROUP BY claim_id
HAVING COUNT(*) > 1;

-- Measure impact
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT claim_id) AS distinct_claims,
    COUNT(*) - COUNT(DISTINCT claim_id) AS duplicate_count,
    ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT claim_id)) / COUNT(*), 2) AS dup_pct
FROM fact_claims;
```

### Solutions

**Solution 1: MERGE / UPSERT (preferred for streaming)**
```sql
-- Snowflake MERGE: idempotent upsert
MERGE INTO fact_claims t
USING (SELECT DISTINCT ON (claim_id) * FROM staging_claims
       ORDER BY claim_id, ingestion_ts DESC) s
ON t.claim_id = s.claim_id
WHEN MATCHED THEN UPDATE SET t.billed_amount = s.billed_amount, ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
```

**Solution 2: Window function deduplication**
```python
# Spark: keep latest record per business key
from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window

window = Window.partitionBy("claim_id").orderBy(desc("ingestion_ts"))

df_deduped = df_raw \
    .withColumn("rn", row_number().over(window)) \
    .filter("rn = 1") \
    .drop("rn")
```

**Solution 3: Hash-based deduplication**
```python
# Add hash of all attributes → dedup by hash
from pyspark.sql.functions import md5, concat_ws

df = df.withColumn(
    "row_hash",
    md5(concat_ws("|", "claim_id", "billed_amount", "status", "claim_date"))
)

# Insert only rows whose hash doesn't exist in target
df_new = df.join(
    existing_hashes_df,
    on="row_hash",
    how="left_anti"   # rows in df that have NO match in existing_hashes_df
)
```

**Solution 4: Delta Lake MERGE with duplicate-safe load**
```python
from delta.tables import DeltaTable

def upsert_to_delta(micro_batch_df, batch_id):
    # Dedup within micro-batch first
    deduped = micro_batch_df.dropDuplicates(["claim_id"])

    DeltaTable.forPath(spark, "s3://silver/claims/") \
        .alias("target") \
        .merge(deduped.alias("source"), "target.claim_id = source.claim_id") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
```

### Prevention
- Enable idempotent Kafka producers (`enable.idempotence=true`)
- Use MERGE not INSERT for all streaming writes
- Checkpoint offsets atomically with data writes (exactly-once)
- Hash_diff in DV2 satellites prevents duplicate satellite rows

### Interview Questions
1. What is the difference between deduplication at source vs at target?
2. How do you deduplicate records within a Spark micro-batch?
3. Why is `left_anti` join used for hash-based deduplication?
4. How does an idempotent Kafka producer prevent duplicates?

---

## 11.3 Schema Drift

### Problem
Source system changes its schema (adds column, renames column, changes data type) without notifying downstream teams. Pipeline breaks or silently loads wrong data.

### Types of Schema Changes
| Change | Severity | Impact |
|---|---|---|
| Add column | Low | Downstream ignores new column (if handled) |
| Rename column | High | Downstream breaks — column not found |
| Drop column | High | Downstream breaks or returns NULLs |
| Change data type | High | Type mismatch error or silent truncation |
| Change column order (CSV) | High | Wrong data in wrong column |
| Add NOT NULL constraint | Medium | NULLs in existing data fail insert |

### Detection
```python
# Compare current schema vs expected schema
from pyspark.sql.types import StructType
import json

expected_schema = StructType.fromJson(json.load(open("expected_schema.json")))
actual_schema = spark.read.parquet("s3://bronze/claims/").schema

# Find differences
expected_cols = {f.name: f.dataType for f in expected_schema.fields}
actual_cols   = {f.name: f.dataType for f in actual_schema.fields}

added    = set(actual_cols) - set(expected_cols)
removed  = set(expected_cols) - set(actual_cols)
changed  = {c for c in expected_cols & actual_cols
            if expected_cols[c] != actual_cols[c]}

if removed or changed:
    raise ValueError(f"Breaking schema change detected!\n"
                     f"Removed: {removed}\nChanged: {changed}")
if added:
    logger.warning(f"New columns detected (non-breaking): {added}")
```

### Solutions

**Solution 1: Schema-on-read tolerance (Bronze layer)**
```python
# Bronze: tolerate schema drift with mergeSchema
df.write.format("delta") \
    .option("mergeSchema", "true") \   # auto-evolve schema
    .mode("append") \
    .save("s3://bronze/claims/")
# New columns land in Bronze with NULLs in older records
# Breaking changes (type change) still fail → catch in DQ
```

**Solution 2: Schema registry (Kafka / Avro)**
```python
# Schema Registry enforces compatibility rules
# BACKWARD: new schema can read data written with old schema
# FORWARD: old schema can read data written with new schema
# FULL: both backward + forward compatible

# Incompatible change (remove field without default) → rejected by registry
# Compatible change (add field with default) → allowed
```

**Solution 3: Explicit schema contract + alerting**
```python
# Great Expectations: schema validation in pipeline
validator.expect_table_columns_to_match_ordered_list([
    "claim_id", "patient_id", "payer_id", "billed_amount", "claim_date"
])
validator.expect_column_values_to_be_of_type("billed_amount", "DecimalType")
# Fails fast if schema drifts — catches before Bronze lands
```

**Solution 4: dbt source freshness + schema tests**
```yaml
# dbt: test source schema on every run
sources:
  - name: ods
    tables:
      - name: claims
        columns:
          - name: claim_id
            tests: [not_null, unique]
          - name: billed_amount
            tests:
              - dbt_expectations.expect_column_values_to_be_of_type:
                  column_type: numeric
```

### Prevention
- Schema Registry for all Kafka topics (reject incompatible changes)
- Contract testing between producer and consumer
- Automated schema comparison on every pipeline run
- Source system change management: notify data team before schema changes

### Interview Questions
1. What is schema drift and what are the most dangerous types of schema changes?
2. How does Delta Lake's `mergeSchema` option help handle schema drift?
3. What is Schema Registry and how does it enforce schema compatibility?
4. How would you design a pipeline that is resilient to additive schema changes but fails fast on breaking changes?

---

## 11.4 Reprocessing Strategy

### Problem
Pipeline ran with wrong logic, wrong data, or failed mid-way. Need to re-run historical data correctly without duplicates or data loss.

### Reprocessing patterns

**Pattern 1: Partition overwrite (most common)**
```python
# Overwrite only affected date partitions
df_corrected.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "claim_date BETWEEN '2024-01-01' AND '2024-01-31'") \
    .save("s3://silver/claims/")
# Only Jan 2024 partition replaced. Other months untouched.
```

**Pattern 2: Full table rebuild**
```python
# When logic change affects all historical data
spark.sql("TRUNCATE TABLE silver_claims")
spark.sql("INSERT INTO silver_claims SELECT * FROM bronze_claims WHERE ...")
# Or: drop + recreate table
```

**Pattern 3: Airflow backfill**
```bash
airflow dags backfill claims_pipeline \
    --start-date 2024-01-01 \
    --end-date 2024-03-31 \
    --reset-dagruns              # clear existing dag runs first
```

**Pattern 4: Kafka replay (Kappa)**
```bash
# Reset consumer group offset to beginning
kafka-consumer-groups.sh \
    --bootstrap-server broker:9092 \
    --group claims-processor \
    --topic claims-topic \
    --reset-offsets \
    --to-earliest \
    --execute
# Stream processor replays all events from Kafka beginning
# Requires Kafka retention covers full history
```

**Pattern 5: Delta Lake time travel for comparison**
```sql
-- Compare current output vs historical output to validate reprocessing
SELECT new.claim_id,
       new.billed_amount AS new_amount,
       old.billed_amount AS old_amount,
       new.billed_amount - old.billed_amount AS delta
FROM fact_claims new
JOIN fact_claims VERSION AS OF 10 old   -- version before reprocessing
    ON new.claim_id = old.claim_id
WHERE new.billed_amount != old.billed_amount
ORDER BY ABS(delta) DESC;
```

### Reprocessing checklist
```
Before reprocessing:
  □ Snapshot current state (Delta clone or version note)
  □ Notify downstream consumers (dashboards will be stale)
  □ Validate pipeline is idempotent (safe to re-run)
  □ Estimate row counts: expected vs actual after reprocessing

During reprocessing:
  □ Monitor: job progress, error rate, row counts per partition
  □ Run in non-production first if possible

After reprocessing:
  □ Row count reconciliation: before vs after
  □ Sum reconciliation on key measures
  □ Downstream DQ checks
  □ Notify consumers: reprocessing complete
```

### Interview Questions
1. What is the difference between backfill and reprocessing?
2. How does Delta Lake's `replaceWhere` enable safe partition-level reprocessing?
3. What preconditions must be true for Airflow backfill to work correctly?
4. How do you validate that reprocessing produced correct results?

---

## 11.5 Multi-Tenant Data Design

### Problem
Multiple customers/payers/business units share the same data platform. Data must be strictly isolated — Payer A cannot see Payer B's data.

### Isolation patterns

**Pattern 1: Schema-per-tenant**
```sql
-- Each tenant gets their own schema
CREATE SCHEMA payer_bcbs;
CREATE SCHEMA payer_aetna;
CREATE SCHEMA payer_cigna;

-- Each schema has same table structure
CREATE TABLE payer_bcbs.fact_claims (...);
CREATE TABLE payer_aetna.fact_claims (...);
-- Simple isolation. But: schema sprawl at hundreds of tenants.
-- ETL complexity: N schemas to load.
```

**Pattern 2: Row-level isolation (single table)**
```sql
-- One shared table with tenant_id column
CREATE TABLE fact_claims (
    claim_id    VARCHAR,
    tenant_id   VARCHAR,    -- payer identifier
    ...
);

-- Row Access Policy enforces isolation
CREATE ROW ACCESS POLICY tenant_isolation
AS (tenant_id VARCHAR) RETURNS BOOLEAN ->
    tenant_id = CURRENT_ACCOUNT()   -- or lookup from user-tenant mapping
;

ALTER TABLE fact_claims ADD ROW ACCESS POLICY tenant_isolation ON (tenant_id);
-- Tenant A's users only see tenant_id = 'PAYER_BCBS' rows
-- Enforced at query layer — zero application code change needed
```

**Pattern 3: Database-per-tenant (strongest isolation)**
```sql
-- Snowflake: separate database per major tenant
CREATE DATABASE payer_bcbs_db;
CREATE DATABASE payer_aetna_db;
-- Full isolation: separate time travel, separate cloning, separate billing
-- Expensive: N databases to manage
-- Best for: very large tenants or strict compliance isolation
```

**Pattern 4: Virtual isolation (views)**
```sql
-- Shared table + tenant-filtered views
CREATE VIEW payer_bcbs.fact_claims AS
SELECT * FROM shared.fact_claims WHERE tenant_id = 'BCBS';
-- View layer provides isolation without physical separation
-- Simpler than RAP for simple use cases
```

### Multi-tenant DQ and monitoring
```sql
-- Per-tenant row count monitoring
SELECT tenant_id,
       COUNT(*) AS row_count,
       SUM(billed_amount) AS total_billed,
       MAX(ingestion_ts) AS last_loaded
FROM fact_claims
GROUP BY tenant_id
ORDER BY row_count DESC;
-- Alert if any tenant shows 0 rows or dramatically lower than yesterday
```

### Common Mistakes
- ❌ Forgetting to add tenant_id to all tables — cross-tenant data leak
- ❌ No index/cluster on tenant_id — full table scan for every tenant query
- ❌ Schema-per-tenant without automation — 200 tenants = 200 manual schema setups
- ❌ Allowing admin users to bypass row access policies — audit accounts with ACCOUNTADMIN
- ✅ Cluster Snowflake tables on (tenant_id, date) — tenant pruning before date pruning
- ✅ Validate: run a query AS each tenant user → confirm only their data is visible

### Interview Questions
1. What are the trade-offs between schema-per-tenant and row-level isolation?
2. How do Snowflake Row Access Policies enable multi-tenant isolation?
3. How would you design a billing model where each tenant sees their own compute cost?
4. What is the risk of using views for tenant isolation vs Row Access Policies?

---

## 11.6 Disaster Recovery

### Theory

**Key metrics:**
- **RPO (Recovery Point Objective):** Maximum acceptable data loss. "We can lose at most 1 hour of data."
- **RTO (Recovery Time Objective):** Maximum acceptable downtime. "System must be back in 4 hours."

**DR strategies:**

| Strategy | RPO | RTO | Cost |
|---|---|---|---|
| Backup + Restore | Hours | Hours | Low |
| Pilot Light | Minutes | 30-60 min | Medium |
| Warm Standby | Minutes | Minutes | Medium-High |
| Multi-Active | Near-zero | Near-zero | Very High |

**Snowflake DR:**
```sql
-- Time Travel: recover accidentally deleted/modified data
-- RPO = 0 to data_retention_time (1-90 days)
CREATE TABLE fact_claims_recovered CLONE fact_claims
AT (TIMESTAMP => '2024-01-15 10:00:00'::TIMESTAMP);

-- Fail-Safe: 7-day recovery window after time travel expires
-- Managed by Snowflake — contact support for fail-safe recovery

-- Business Continuity: Snowflake replication to secondary region
CREATE REPLICATION GROUP primary_rg
    OBJECT_TYPES = DATABASES
    ALLOWED_ACCOUNTS = my_org.my_secondary_account;

-- Failover (if primary region down)
ALTER REPLICATION GROUP primary_rg PRIMARY;
-- Secondary becomes primary. RPO = replication lag (minutes).
```

**S3/Data Lake DR:**
```
Option 1: S3 Cross-Region Replication (CRR)
  • Automatic async replication to secondary region
  • RPO: minutes (replication lag)
  • Enable versioning + CRR on S3 bucket
  • Cost: storage in 2 regions + replication data transfer

Option 2: Delta Lake + S3 CRR
  • Delta log replicated with data files
  • Secondary region can query Delta table read-only
  • On failover: redirect compute to secondary region

Option 3: Iceberg + Object Storage mirroring
  • Iceberg catalog replicated (Nessie/REST catalog)
  • Data files replicated via CRR
  • Any engine in secondary region reads Iceberg
```

**Pipeline DR (Airflow):**
```python
# Multi-region Airflow deployment
# Primary: us-east-1 MWAA
# Secondary: us-west-2 MWAA (same DAGs deployed, paused)

# On failover:
# 1. Pause all DAGs in primary (or primary is down)
# 2. Unpause secondary MWAA DAGs
# 3. Update DNS to point pipeline triggers to secondary
# 4. Verify: checkpoint locations (S3) accessible from secondary region

# DAG checkpoint and state must be in shared S3 (not region-local)
# Airflow metadata DB: RDS Multi-AZ for HA within region
```

**DR Testing:**
```
Quarterly DR drill:
  □ Simulate primary region failure
  □ Failover to secondary region
  □ Validate: all pipelines start in secondary
  □ Validate: data accessible in secondary
  □ Measure actual RTO vs target
  □ Document gaps → remediation plan
  □ Failback to primary
  □ Update runbook
```

### Common Mistakes
- ❌ DR plan never tested — runbook is theoretical; actual failover takes 10x longer
- ❌ Backups to same region as primary — region failure = lose backups too
- ❌ RPO/RTO set without business input — DR is a business decision, not IT decision
- ❌ Snowflake Time Travel as DR strategy — not a backup; admin errors can destroy data within retention window
- ✅ Test DR quarterly with actual failover drill
- ✅ Cross-region replication for all critical data stores
- ✅ Document RTO/RPO per system, per criticality tier

### Interview Questions
1. What is the difference between RPO and RTO? Give a real example.
2. How does Snowflake Replication support disaster recovery?
3. What is the difference between Time Travel and Fail-Safe in Snowflake for DR purposes?
4. How would you design a DR strategy for a Kafka + Delta Lake + Airflow pipeline?
5. Why is it insufficient to rely on Snowflake Time Travel as your only DR mechanism?

---

## 11.7 Slowly Changing Streams

### Problem
Streaming pipeline receives updates to dimension-like entities (customer profile changes, price list updates) mixed with high-volume transactional events. How do you enrich streaming transactions with current AND historical dimension state?

### Solution patterns

**Pattern 1: Broadcast dimension to streaming job**
```python
# Load small, slowly-changing dim into memory
# Refresh periodically (hourly, not per-event)
dim_payer_df = spark.read.parquet("s3://gold/dim_payer/").cache()
dim_payer_broadcast = broadcast(dim_payer_df)

# Enrich streaming claims with payer dim
enriched_stream = claims_stream.join(
    dim_payer_broadcast,
    on="payer_id",
    how="left"
)
# Works for: dims < 1GB, change rate < hourly
# Refresh: restart streaming job or re-cache periodically
```

**Pattern 2: Lookup join with temporal versioning**
```python
# As-of join: for each event, find dim row valid at event_time
# Flink supports temporal joins natively

# Flink SQL: temporal join
SELECT
    c.claim_id, c.claim_date, c.amount,
    p.payer_name, p.contract_rate      -- dim value valid AT claim_date
FROM claims c
JOIN payer_history FOR SYSTEM_TIME AS OF c.claim_date p
ON c.payer_id = p.payer_id
-- Uses dim version that was valid when the claim occurred
```

**Pattern 3: Delta Change Data Feed (CDF) for dim sync**
```python
# Enable CDF on dimension table
spark.sql("ALTER TABLE dim_payer SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Streaming job consumes ONLY dim changes
dim_changes = spark.readStream \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("dim_payer")
# Enrich transactions by joining with latest known dim state
```

### Interview Questions
1. How do you enrich a high-volume event stream with slowly-changing dimension data?
2. What is a temporal join in Flink and when is it needed?
3. What is Delta Change Data Feed and how does it support streaming dimension updates?

---

*Next: Chunk 12 — Interview Master (Scenario questions, tradeoff analysis, architecture design, hands-on labs)*
# DE Master Doc — Chunk 12: Interview Master
> Scenario-based questions · Tradeoff analysis · Architecture design · Hands-on labs

---

## 12.1 Scenario-Based Questions

### Scenario 1: Design a Real-Time Fraud Detection Pipeline
**"Design a system that detects fraudulent insurance claims in under 2 seconds."**

```
Interviewer wants to hear:
  1. Streaming architecture choice and justification
  2. Feature engineering for ML
  3. Low-latency serving
  4. Fallback / degraded mode
  5. How you handle model updates without downtime

Answer structure:

INGESTION:
  EHR/Claims system → Kafka (claim_submitted topic)
  Avro + Schema Registry (schema contract)

STREAM PROCESSING (Flink, < 100ms latency):
  1. Parse and validate event
  2. Enrich with patient history features:
     - Lookup Redis: patient_claim_count_30d, avg_claim_amount_90d
     - Lookup Redis: provider risk score (updated daily from batch)
  3. Apply ML model (Flink embedded, PMML or ONNX):
     - Features: claim_amount, diagnosis_code, provider_risk, patient_history
     - Output: fraud_probability score
  4. Threshold decision: score > 0.85 → flag for review
  5. Emit to: kafka (claims_flagged) + Redis (real-time status)

FEATURE STORE:
  Batch features (daily Spark job) → Redis / DynamoDB
  Streaming features (Flink) → Redis (patient real-time counts)

ML MODEL SERVING:
  Option A: Flink embedded model (lowest latency, no network hop)
  Option B: gRPC model server (TensorFlow Serving / BentoML)
  Model versioning: MLflow registry → promote to production

RESULT SINK:
  Kafka (claims_flagged) → Claims adjudication system
  Delta Lake (audit trail, retraining data)

MONITORING:
  Kafka consumer lag < 1000 messages
  Flink checkpoint duration < 30s
  Model: precision/recall drift monitoring (Evidently AI)
  Alert: PagerDuty on Flink job failure

ARCHITECTURE:
  Claim → Kafka → Flink (enrich + score) → Redis + Kafka + Delta
                      ↑
               Redis (features) ← Spark batch (daily feature refresh)
```

---

### Scenario 2: Design a Healthcare Data Warehouse from Scratch
**"Design a DWH for a health insurance company processing 10M claims/day."**

```
LAYERS:
  Bronze (Raw Vault): Debezium CDC from Oracle EHR → Kafka → Delta Lake
  Silver (Business Vault): DV2 Hubs/Links/Satellites in Snowflake
  Gold (Information Marts): Kimball star schemas for BI

MODELING:
  Bus Matrix: Claims, Eligibility, Encounters, Prior Auth
  Conformed dims: dim_patient, dim_provider, dim_payer, dim_date, dim_diagnosis
  Fact tables: fact_claims (transaction), fact_eligibility (snapshot)

TOOLING:
  Ingestion:      Informatica IICS (from legacy) + Debezium (CDC)
  Transformation: dbt Core (SQL) + Spark (Python, large transforms)
  Orchestration:  Apache Airflow (MWAA)
  DWH:            Snowflake (multi-cluster, Enterprise)
  BI:             Power BI (Direct Lake via Snowflake connector)
  Governance:     Collibra DGC + Snowflake masking + RAP
  DQ:             Great Expectations + dbt tests

SCALABILITY (10M claims/day = ~115 claims/second):
  Snowflake multi-cluster: scale out for concurrent BI users
  dbt parallel model runs: multiple threads
  Partitioning: fact_claims by claim_date, clustered by payer_id
  Result cache: repeated dashboard queries → instant response

COMPLIANCE (HIPAA):
  PHI masking: SSN, DOB, name → dynamic masking by role
  Row-level security: payer analysts see only their payer's data
  Audit: Snowflake Access History → Collibra lineage
  Encryption: Tri-Secret Secure (Snowflake + customer KMS key)
  Retention: 7 years per HIPAA requirement
```

---

### Scenario 3: Your Snowflake Query Takes 45 Minutes — Debug It
**"A critical daily report takes 45 minutes. Walk me through your debugging process."**

```
Step 1: Query Profile (Snowflake UI)
  → Check: Partitions scanned vs total (pruning effective?)
  → Check: Bytes spilled to remote storage (memory overflow?)
  → Check: Most expensive node (bottleneck operator)
  → Check: Join type (broadcast? hash? sort-merge?)

Step 2: Common findings and fixes

Finding A: "Partitions scanned = 100% of partitions"
  → No pruning happening
  → Fix: check WHERE clause for functions (YEAR(date) → use BETWEEN)
  → Fix: add CLUSTER BY on filter columns
  → Fix: check if partition pruning column matches cluster key

Finding B: "Bytes spilled to remote storage = 500GB"
  → Warehouse too small for data volume
  → Fix: scale up warehouse (S → L → XL)
  → Fix: rewrite query to reduce intermediate result size
  → Fix: break into smaller CTEs + intermediate tables

Finding C: "Slow join — 8B rows × 2B rows"
  → Missing join filter / cartesian product risk
  → Fix: add filter before join (reduce probe side)
  → Fix: check join condition (is it truly selective?)

Finding D: "Repeated expensive subquery"
  → Subquery evaluated once per row
  → Fix: materialize as CTE or temp table
  → Fix: create materialized view if query runs daily

Step 3: Query rewrite
  BEFORE: SELECT * FROM a JOIN b ON ... JOIN c ON ... WHERE YEAR(a.date)=2024
  AFTER:
    WITH filtered AS (
      SELECT key, col FROM a
      WHERE a.date BETWEEN '2024-01-01' AND '2024-12-31'  -- range not function
    )
    SELECT f.col, b.col, c.col
    FROM filtered f
    JOIN b ON f.key = b.key                -- smaller probe side
    JOIN c ON b.key2 = c.key2;

Step 4: Structural fixes
  → Add clustering: ALTER TABLE a CLUSTER BY (date, region);
  → Add materialized view for repeated aggregation
  → Right-size warehouse: run on XL, check if spillage eliminated
```

---

### Scenario 4: Data Pipeline Producing Wrong Numbers
**"Finance says the monthly revenue dashboard shows $50M but the source system shows $52M. Debug."**

```
Systematic root cause framework:

1. QUANTIFY THE GAP
   Expected: $52M (source)
   Actual:   $50M (dashboard)
   Delta:    -$2M (what's missing?)

2. NARROW DOWN BY DIMENSION
   By payer:     Is it all payers or specific payers?
   By date:      Is it all months or specific periods?
   By claim type: Transaction type missing?
   → Isolate the subset where discrepancy exists

3. TRACE THE LINEAGE
   Dashboard → Gold mart → Silver → Bronze → Source
   Check row counts at each layer:
     Source:  5,200,000 claims ($52M)
     Bronze:  5,200,000 claims ($52M) ✓ ingestion OK
     Silver:  5,000,000 claims ($50M) ← DROP HAPPENS HERE
     Gold:    5,000,000 claims ($50M)
   → 200,000 claims dropped in Bronze→Silver transform

4. FIND THE FILTER
   silver_claims = bronze_claims
     .filter("claim_amount > 0 AND status = 'APPROVED'")
   → $2M in claims with status = 'PENDING' were filtered out
   → Business requirement: PENDING claims should be included

5. FIX + VALIDATE
   Fix: remove status filter OR include PENDING status
   Reprocess: backfill Silver and Gold for affected months
   Validate: source $52M == Gold $52M ✓

6. PREVENT RECURRENCE
   Add DQ check: SUM(gold.revenue) within 1% of SUM(source.revenue)
   Add to pipeline: reconciliation job runs after every load
   Document: business rule about which status codes are included
```

---

## 12.2 "Why This Over That?" — Decision Framework

### Why dbt over stored procedures?
```
Stored procs:
  ❌ No version control (lives in DB, not Git)
  ❌ No automated testing
  ❌ No documentation
  ❌ No lineage tracking
  ❌ Hard to review (no diff)

dbt:
  ✅ SQL in Git (version control, PR review, CI/CD)
  ✅ Built-in testing (not_null, unique, relationships, custom)
  ✅ Auto-generated documentation + lineage DAG
  ✅ Modular (ref() builds dependency graph)
  ✅ Jinja templating for DRY SQL

When stored procs still win:
  → Complex procedural logic with loops/cursors not possible in SQL
  → Legacy systems already heavily invested in stored procs
  → Performance-critical code that benefits from DB-side execution
```

### Why Data Vault 2.0 over Kimball for the integration layer?
```
Kimball alone:
  ❌ Schema changes (new source) = redesign fact/dim tables
  ❌ Two sources for same entity = complex ETL reconciliation
  ❌ Hard to trace: "which source provided this value?"
  ❌ Business rules baked into integration = hard to change

DV2 + Kimball info mart:
  ✅ Add new source = add new satellite (zero impact to existing)
  ✅ Multi-source: each source = separate satellite on same hub
  ✅ Full audit: load_date, record_source on every row
  ✅ Business rules only in Business Vault / Info Mart
  ✅ Insert-only = append to cloud storage (fast parallel loads)

When Kimball alone is fine:
  → Single source of truth
  → Small team, simple model
  → Fast time-to-value required
  → Performance is top priority
```

### Why Iceberg over Delta Lake for new projects?
```
Delta Lake:
  ✅ Mature, proven at scale (Databricks)
  ✅ Best Databricks integration (Photon, Unity Catalog)
  ❌ Databricks-leaning governance
  ❌ Multi-engine support still maturing

Iceberg:
  ✅ Truly open (Apache foundation)
  ✅ Best multi-engine (Spark + Flink + Trino + Athena + Snowflake + Dremio)
  ✅ Hidden partitioning (no partition evolution pain)
  ✅ Native Snowflake Iceberg tables
  ❌ Slightly less mature ecosystem vs Delta

Choose Iceberg when:
  → Multi-cloud strategy (avoid lock-in)
  → Multiple query engines on same data
  → Snowflake as one of the engines

Choose Delta when:
  → Databricks is primary platform
  → Photon acceleration needed
  → Unity Catalog for governance
```

### Why Kafka over direct database integration?
```
Direct DB integration (point-to-point):
  ❌ N systems = N² connections (spaghetti)
  ❌ Tight coupling — system A knows about system B
  ❌ Source DB handles all consumer load
  ❌ No replay — consumer missed data = gone
  ❌ No schema contract enforcement

Kafka:
  ✅ Hub-and-spoke: producers don't know consumers
  ✅ Decoupled: add consumer without touching producer
  ✅ Replay: any consumer can replay from any offset
  ✅ Buffering: consumer lag = Kafka buffers; no data loss
  ✅ Schema Registry: contract enforcement
  ✅ Multi-consumer: same topic → many consumers independently

When to NOT use Kafka:
  → Simple, low-volume point-to-point integration
  → Small team without Kafka operational expertise
  → Latency > 1 minute acceptable → scheduled batch simpler
```

---

## 12.3 Hands-On Labs Reference

### Lab 1: Snowflake — Build a Claims DWH

```sql
-- Step 1: Create database structure
CREATE DATABASE claims_dw;
CREATE SCHEMA claims_dw.raw;
CREATE SCHEMA claims_dw.silver;
CREATE SCHEMA claims_dw.gold;

-- Step 2: Create dim_date (pre-populated calendar)
CREATE TABLE claims_dw.gold.dim_date AS
SELECT
    DATEADD(day, seq4(), '2020-01-01')::DATE          AS full_date,
    TO_NUMBER(TO_CHAR(DATEADD(day, seq4(), '2020-01-01'), 'YYYYMMDD')) AS date_sk,
    YEAR(DATEADD(day, seq4(), '2020-01-01'))           AS year,
    MONTH(DATEADD(day, seq4(), '2020-01-01'))          AS month,
    DAY(DATEADD(day, seq4(), '2020-01-01'))            AS day,
    QUARTER(DATEADD(day, seq4(), '2020-01-01'))        AS quarter,
    DAYOFWEEK(DATEADD(day, seq4(), '2020-01-01'))      AS day_of_week,
    CASE WHEN DAYOFWEEK(DATEADD(day, seq4(), '2020-01-01')) IN (0,6)
         THEN TRUE ELSE FALSE END                       AS is_weekend
FROM TABLE(GENERATOR(ROWCOUNT => 3650));  -- 10 years

-- Step 3: Create fact table with clustering
CREATE TABLE claims_dw.gold.fact_claims (
    claim_sk          BIGINT AUTOINCREMENT PRIMARY KEY,
    claim_nk          VARCHAR(50),
    patient_sk        INT,
    payer_sk          INT,
    provider_sk       INT,
    service_date_sk   INT,
    billed_amount     DECIMAL(12,2),
    paid_amount       DECIMAL(12,2),
    processing_days   INT
) CLUSTER BY (service_date_sk, payer_sk);

-- Step 4: Create SCD Type 2 dim
CREATE TABLE claims_dw.gold.dim_patient (
    patient_sk        INT AUTOINCREMENT PRIMARY KEY,
    patient_nk        VARCHAR(50)  NOT NULL,
    patient_name      VARCHAR(200),
    state_code        CHAR(2),
    insurance_plan    VARCHAR(100),
    eff_start_date    DATE         NOT NULL,
    eff_end_date      DATE,
    is_current        BOOLEAN      DEFAULT TRUE,
    row_hash          VARCHAR(64)  -- MD5 of tracked attributes
);

-- Step 5: Mask PII
CREATE MASKING POLICY claims_dw.silver.mp_name_mask
AS (val STRING) RETURNS STRING ->
    CASE WHEN IS_ROLE_IN_SESSION('PII_VIEWER') THEN val
         ELSE CONCAT(LEFT(val,1), '***') END;

-- Step 6: Row access policy
CREATE ROW ACCESS POLICY claims_dw.gold.rap_payer
AS (payer_id VARCHAR) RETURNS BOOLEAN ->
    payer_id IN (
        SELECT payer_id FROM claims_dw.raw.user_payer_access
        WHERE username = CURRENT_USER()
    );

ALTER TABLE claims_dw.gold.fact_claims
ADD ROW ACCESS POLICY claims_dw.gold.rap_payer ON (payer_id);
```

---

### Lab 2: dbt — Build Mart Models

```yaml
# dbt_project.yml
name: claims_dw
version: '1.0.0'
profile: snowflake_prod

models:
  claims_dw:
    staging:
      +schema: silver
      +materialized: view
    marts:
      +schema: gold
      +materialized: table

# models/schema.yml
models:
  - name: fact_claims
    description: "Grain: one row per claim transaction"
    columns:
      - name: claim_sk
        tests: [unique, not_null]
      - name: patient_sk
        tests:
          - relationships:
              to: ref('dim_patient')
              field: patient_sk
      - name: billed_amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 10000000
```

```sql
-- models/staging/stg_claims.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'claims') }}
    WHERE _loaded_at >= DATEADD('day', -3, CURRENT_DATE)  -- incremental
),
cleaned AS (
    SELECT
        claim_id                              AS claim_nk,
        patient_id                            AS patient_nk,
        payer_id                              AS payer_nk,
        CAST(claim_date AS DATE)              AS claim_date,
        UPPER(TRIM(status_code))              AS status_code,
        COALESCE(billed_amount, 0)            AS billed_amount,
        COALESCE(paid_amount, 0)              AS paid_amount,
        MD5(CONCAT(claim_id, billed_amount, status_code)) AS row_hash
    FROM source
    WHERE claim_id IS NOT NULL
)
SELECT * FROM cleaned

-- models/marts/fact_claims.sql
{{ config(
    materialized='incremental',
    unique_key='claim_nk',
    cluster_by=['service_date_sk', 'payer_sk']
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.claim_nk']) }} AS claim_sk,
    c.claim_nk,
    p.patient_sk,
    py.payer_sk,
    pr.provider_sk,
    d.date_sk         AS service_date_sk,
    c.billed_amount,
    c.paid_amount,
    DATEDIFF('day', c.claim_date, c.paid_date) AS processing_days
FROM {{ ref('stg_claims') }} c
JOIN {{ ref('dim_patient') }} p  ON c.patient_nk  = p.patient_nk AND p.is_current
JOIN {{ ref('dim_payer') }}   py ON c.payer_nk    = py.payer_nk
JOIN {{ ref('dim_provider') }} pr ON c.provider_nk = pr.provider_nk
JOIN {{ ref('dim_date') }}    d  ON c.claim_date  = d.full_date

{% if is_incremental() %}
WHERE c.claim_date >= (SELECT MAX(service_date_sk)::VARCHAR::DATE FROM {{ this }})
{% endif %}
```

---

### Lab 3: Kafka + Debezium CDC Pipeline

```yaml
# docker-compose.yml (dev environment)
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    environment:
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka:9092
      SCHEMA_REGISTRY_HOST_NAME: schema-registry

  debezium:
    image: debezium/connect:2.4
    depends_on: [kafka, schema-registry]
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: debezium-connect
      CONFIG_STORAGE_TOPIC: debezium_configs
      OFFSET_STORAGE_TOPIC: debezium_offsets
```

```bash
# Register Debezium PostgreSQL connector
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "claims-postgres-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "dbz",
      "database.dbname": "claims_db",
      "database.server.name": "claims_postgres",
      "table.include.list": "public.claims,public.patients",
      "plugin.name": "pgoutput",
      "publication.name": "debezium_pub",
      "value.converter": "io.confluent.kafka.serializers.KafkaAvroSerializer",
      "value.converter.schema.registry.url": "http://schema-registry:8081",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.add.fields": "op,ts_ms,source.table"
    }
  }'

# Verify connector running
curl http://localhost:8083/connectors/claims-postgres-connector/status

# Consume CDC events
kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic claims_postgres.public.claims \
  --from-beginning
```

---

### Lab 4: Spark + Delta Lake Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("ClaimsPipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# --- BRONZE: ingest raw ---
df_raw = spark.read.json("s3://raw/claims/2024/01/15/")

df_bronze = df_raw \
    .withColumn("ingestion_ts", current_timestamp()) \
    .withColumn("ingestion_date", current_date())

df_bronze.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .partitionBy("ingestion_date") \
    .mode("append") \
    .save("s3://datalake/bronze/claims/")

# --- SILVER: cleanse + dedup ---
window = Window.partitionBy("claim_id").orderBy(desc("ingestion_ts"))

df_silver = (
    spark.read.format("delta").load("s3://datalake/bronze/claims/")
    .filter("ingestion_date = current_date()")
    .withColumn("rn", row_number().over(window))
    .filter("rn = 1").drop("rn")
    .filter("claim_amount > 0 AND claim_id IS NOT NULL AND patient_id IS NOT NULL")
    .withColumn("claim_date", to_date("claim_date_str", "yyyy-MM-dd"))
    .withColumn("status_code", upper(trim(col("status_code"))))
    .withColumn("row_hash", md5(concat_ws("|",
        "claim_id", "claim_amount", "status_code", "payer_id")))
)

# MERGE silver (upsert — idempotent)
if DeltaTable.isDeltaTable(spark, "s3://datalake/silver/claims/"):
    silver_table = DeltaTable.forPath(spark, "s3://datalake/silver/claims/")
    (silver_table.alias("t")
        .merge(df_silver.alias("s"), "t.claim_id = s.claim_id")
        .whenMatchedUpdate(
            condition="t.row_hash != s.row_hash",
            set={"claim_amount": "s.claim_amount",
                 "status_code": "s.status_code",
                 "row_hash": "s.row_hash",
                 "updated_ts": "s.ingestion_ts"})
        .whenNotMatchedInsertAll()
        .execute())
else:
    df_silver.write.format("delta").save("s3://datalake/silver/claims/")

# --- OPTIMIZE: compact small files + Z-order ---
spark.sql("""
    OPTIMIZE delta.`s3://datalake/silver/claims/`
    ZORDER BY (payer_id, diagnosis_code)
""")

# --- GOLD: aggregation ---
df_gold = (
    spark.read.format("delta").load("s3://datalake/silver/claims/")
    .groupBy("payer_id", year("claim_date").alias("claim_year"),
             month("claim_date").alias("claim_month"))
    .agg(
        sum("claim_amount").alias("total_billed"),
        sum("paid_amount").alias("total_paid"),
        count("claim_id").alias("claim_count"),
        avg("processing_days").alias("avg_processing_days"),
        countDistinct("patient_id").alias("unique_patients")
    )
)

df_gold.write.format("delta").mode("overwrite") \
    .option("replaceWhere", "claim_year = 2024 AND claim_month = 1") \
    .save("s3://datalake/gold/claims_monthly_summary/")
```

---

### Lab 5: Airflow DAG with DQ

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import great_expectations as gx

def run_dq_check(**context):
    ge_context = gx.get_context()
    result = ge_context.run_checkpoint(
        checkpoint_name="claims_silver_checkpoint",
        batch_request={
            "datasource_name": "s3_claims",
            "data_asset_name": "silver_claims",
            "data_connector_query": {"batch_identifiers": {"date": context["ds"]}}
        }
    )
    context["ti"].xcom_push("dq_success", result.success)
    return "proceed_to_gold" if result.success else "quarantine_bad_data"

def quarantine(**context):
    # Move bad rows to quarantine table
    # Alert data engineering team
    pass

with DAG(
    "claims_full_pipeline",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": send_slack_alert,
    },
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    start          = EmptyOperator(task_id="start")
    ingest_bronze  = PythonOperator(task_id="ingest_bronze", python_callable=ingest_fn)
    transform_silver = PythonOperator(task_id="transform_silver", python_callable=transform_fn)

    dq_check = BranchPythonOperator(
        task_id="dq_check",
        python_callable=run_dq_check,
    )

    proceed_to_gold    = PythonOperator(task_id="proceed_to_gold",   python_callable=build_gold)
    quarantine_data    = PythonOperator(task_id="quarantine_bad_data", python_callable=quarantine)
    dbt_run            = BashOperator(task_id="dbt_run",
                             bash_command="dbt run --select marts.claims --target prod")
    notify_success     = PythonOperator(task_id="notify_success",    python_callable=notify_fn)
    end                = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    start >> ingest_bronze >> transform_silver >> dq_check
    dq_check >> proceed_to_gold >> dbt_run >> notify_success >> end
    dq_check >> quarantine_data >> end
```

---

## 12.4 Principal Architect Interview — Final Checklist

### Technical depth signals (what separates Senior from Principal):

```
Senior DE knows:                    Principal Architect adds:

Parquet is columnar                 Row group size tuning, codec choice by workload
Delta Lake has ACID                 Transaction log internals, checkpoint mechanics
Kafka consumer groups               ISR, acks=all, idempotent producer, transaction API
Airflow has DAGs                    Executor architecture, scheduler internals, AQE
SCD Type 2 exists                   hash_diff optimization, dbt snapshot config, backfill implications
Data Vault has Hubs/Links/Sats      PIT table query mechanics, satellite split strategy
Snowflake has VWs                   Caching hierarchy, micro-partition internals, Tri-Secret
BigQuery is serverless              Slot reservation, BI Engine, CMEK, streaming buffer limits
Medallion has 3 layers              Bronze schema drift tolerance, Silver DQ placement, Gold fan-out
```

### 5 questions that separate Principal from Senior:

1. **"Walk me through how you'd diagnose a 10x query slowdown in Snowflake without access to the query."**
   → Shows: systematic debugging, knowledge of Query Profile, caching, pruning, spillage

2. **"Your Data Vault satellite has 500M rows and PIT queries are slow. How do you fix it?"**
   → Shows: PIT table design, satellite split strategy, Bridge tables, clustering

3. **"Design the key management strategy for a HIPAA-compliant multi-tenant Snowflake DWH."**
   → Shows: Tri-Secret Secure, KMS integration, key rotation, per-tenant isolation

4. **"Your Kafka consumer lag is growing. Walk me through your investigation."**
   → Shows: consumer group lag metrics, backpressure, partition skew, consumer scaling

5. **"A business stakeholder says 'the numbers don't match the source system.' Give me your 6-step process."**
   → Shows: structured debugging, lineage tracing, reconciliation methodology

---

## 12.5 Architecture Diagram — Complete DE Reference Stack

```
                        ┌──────────────────────┐
                        │   SOURCE SYSTEMS     │
                        │  Oracle | EHR | SaaS │
                        │  APIs   | Files | IoT│
                        └──────────┬───────────┘
                                   │
               ┌───────────────────┼───────────────────┐
               │ Batch (IICS/ADF)  │ CDC (Debezium)     │ Stream (Kafka)
               ▼                   ▼                     ▼
        ┌──────────────────────────────────────────────────────┐
        │                  BRONZE LAYER                        │
        │         Delta Lake / Iceberg on S3/ADLS              │
        │   Raw | Append-only | mergeSchema | Partitioned      │
        └──────────────────────┬───────────────────────────────┘
                               │ dbt / Spark (DQ + cleanse)
                               ▼
        ┌──────────────────────────────────────────────────────┐
        │                  SILVER LAYER                        │
        │         Delta Lake on S3 / Snowflake                 │
        │   Deduped | DQ-checked | Schema-enforced | Conformed │
        └──────────────────────┬───────────────────────────────┘
                               │ dbt models / Spark aggregation
                               ▼
        ┌──────────────────────────────────────────────────────┐
        │                   GOLD LAYER                         │
        │    ┌────────────┐  ┌──────────────┐  ┌───────────┐  │
        │    │ Star Schema│  │ ML Features  │  │ Regulatory│  │
        │    │ (Snowflake)│  │ (Delta Lake) │  │ Reports   │  │
        │    └──────┬─────┘  └──────┬───────┘  └─────┬─────┘  │
        └──────────┼───────────────┼────────────────┼──────────┘
                   │               │                │
                   ▼               ▼                ▼
             Power BI /      ML Models         Compliance
             Tableau         (MLflow)          Dashboards

ORCHESTRATION:  Apache Airflow (MWAA / Cloud Composer)
GOVERNANCE:     Collibra DGC / Microsoft Purview / Unity Catalog
DQ:             Great Expectations + dbt tests + Monte Carlo
LINEAGE:        OpenLineage → DataHub / Collibra
SECURITY:       Snowflake masking + RAP | Lake Formation | Purview
MONITORING:     Datadog / CloudWatch | PagerDuty | Slack alerts
```

---

*End of DE Master Doc — 12 Chunks Complete*
*Built by Malay Kumar Padhi | linkedin.com/in/mkpadhi | github.com/Techy-Malay*
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
# DE Master Doc — Chunk 14: dbt Advanced + Data Contracts + Quick Reference
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 14.1 dbt Advanced Patterns

### Theory

**dbt project structure (production-grade):**
```
de_project/
├── dbt_project.yml           ← project config, model materializations
├── profiles.yml              ← connection profiles (not in Git — secrets)
├── packages.yml              ← dbt-utils, dbt-expectations, etc.
├── models/
│   ├── staging/              ← stg_* models: light cleanse from source
│   │   ├── schema.yml        ← source definitions + staging tests
│   │   └── stg_claims.sql
│   ├── intermediate/         ← int_* models: joins, business logic
│   │   └── int_claims_enriched.sql
│   └── marts/
│       ├── claims/
│       │   ├── schema.yml    ← mart model tests + column docs
│       │   ├── fact_claims.sql
│       │   └── dim_patient.sql
│       └── finance/
├── snapshots/                ← SCD Type 2 (dbt snapshot)
│   └── dim_patient_snapshot.sql
├── seeds/                    ← static CSV data (lookup tables, test data)
│   └── dim_date.csv
├── tests/                    ← custom singular tests
│   └── assert_claims_reconcile.sql
├── macros/                   ← reusable Jinja macros
│   ├── generate_schema_name.sql
│   └── hash_surrogate_key.sql
├── analyses/                 ← ad-hoc SQL (not materialized)
└── exposures.yml             ← downstream consumers (BI reports, ML)
```

---

**Incremental strategies — the most important dbt topic:**

```sql
-- models/marts/fact_claims.sql

{{ config(
    materialized='incremental',
    unique_key='claim_nk',                    -- dedup key
    incremental_strategy='merge',             -- strategy
    cluster_by=['service_date_sk', 'payer_sk']
) }}

SELECT
    claim_nk,
    patient_sk,
    payer_sk,
    billed_amount,
    paid_amount,
    claim_date
FROM {{ ref('stg_claims') }}

{% if is_incremental() %}
-- Only process new/changed records on incremental runs
WHERE claim_date >= (
    SELECT DATEADD('day', -3, MAX(claim_date))  -- 3-day lookback for late data
    FROM {{ this }}
)
{% endif %}
```

**Four incremental strategies:**

| Strategy | How | Best For | Requires unique_key |
|---|---|---|---|
| `append` | INSERT only new rows | Immutable events, logs | No |
| `merge` | MERGE (upsert) new+changed | Slowly changing data, CDC | Yes |
| `delete+insert` | DELETE then INSERT matching partition | Full partition refresh | Yes |
| `insert_overwrite` | Overwrite matching partitions | Spark/BigQuery large tables | No |

```sql
-- append: fastest, no dedup (good for immutable events)
{{ config(materialized='incremental', incremental_strategy='append') }}

-- merge: upsert by unique_key (Snowflake, BigQuery, Databricks)
{{ config(materialized='incremental',
          incremental_strategy='merge',
          unique_key='claim_nk',
          merge_update_columns=['billed_amount','status','updated_ts']) }}
-- merge_update_columns: only update THESE columns on match (not all)

-- delete+insert: delete matching rows, re-insert (good for partition refresh)
{{ config(materialized='incremental',
          incremental_strategy='delete+insert',
          unique_key=['claim_date', 'payer_id'],
          partition_by={'field': 'claim_date', 'data_type': 'date'}) }}

-- insert_overwrite: Spark/BigQuery — overwrite entire partition
{{ config(materialized='incremental',
          incremental_strategy='insert_overwrite',
          partition_by={'field': 'claim_date', 'data_type': 'date',
                        'granularity': 'day'}) }}
```

---

**dbt macros — reusable Jinja:**
```sql
-- macros/hash_surrogate_key.sql
{% macro hash_surrogate_key(column_names) %}
    MD5(CONCAT_WS('|',
        {% for col in column_names %}
            COALESCE(UPPER(TRIM(CAST({{ col }} AS VARCHAR))), 'NULL')
            {%- if not loop.last %},{% endif %}
        {% endfor %}
    ))
{% endmacro %}

-- Usage in model
SELECT
    {{ hash_surrogate_key(['patient_id', 'source_system']) }} AS patient_hk,
    patient_id,
    patient_name
FROM {{ source('raw', 'patients') }}

-- macros/generate_schema_name.sql (override default schema logic)
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
-- Without override: prod_staging, prod_marts (target.schema + custom_schema)
-- With override: staging, marts (clean schema names)
```

---

**dbt packages — extend dbt:**
```yaml
# packages.yml
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
  - package: calogica/dbt_expectations
    version: [">=0.9.0", "<1.0.0"]
  - package: dbt-labs/dbt_audit_helper
    version: [">=0.9.0"]
  - package: brooklyn-data/dbt_artifacts
    version: [">=2.0.0"]

# Install: dbt deps

# dbt_utils usage:
{{ dbt_utils.generate_surrogate_key(['claim_id', 'source_system']) }}
{{ dbt_utils.star(from=ref('stg_claims'), except=['_loaded_at']) }}
{{ dbt_utils.date_spine(datepart='day', start_date="'2020-01-01'", end_date="'2030-12-31'") }}

# dbt_expectations usage:
- dbt_expectations.expect_column_values_to_be_between:
    min_value: 0
    max_value: 10000000
- dbt_expectations.expect_table_row_count_to_be_between:
    min_value: 1000
    max_value: 10000000
- dbt_expectations.expect_column_pair_values_A_to_be_greater_than_B:
    column_A: paid_amount
    column_B: 0
```

---

**Exposures — document downstream consumers:**
```yaml
# exposures.yml
exposures:
  - name: claims_executive_dashboard
    type: dashboard
    maturity: high
    url: https://powerbi.com/reports/claims-exec
    description: "Executive KPI dashboard for claims operations"
    depends_on:
      - ref('fact_claims')
      - ref('dim_patient')
      - ref('dim_payer')
    owner:
      name: Claims Analytics Team
      email: claims-analytics@company.com

  - name: fraud_ml_model
    type: ml
    maturity: medium
    description: "Fraud detection model trained on claims features"
    depends_on:
      - ref('ml_claims_features')
    owner:
      name: Data Science Team
      email: datascience@company.com
```

---

**dbt Semantic Layer / MetricFlow:**
```yaml
# models/metrics/schema.yml
metrics:
  - name: total_claim_amount
    label: "Total Billed Amount"
    model: ref('fact_claims')
    description: "Sum of all billed claim amounts"
    type: sum
    type_params:
      measure: billed_amount
    dimensions:
      - payer_id
      - claim_date
    filter: "{{ Dimension('status_code') }} = 'APPROVED'"

  - name: claim_count
    label: "Number of Claims"
    model: ref('fact_claims')
    type: count
    type_params:
      measure: claim_id

  - name: approval_rate
    label: "Claim Approval Rate"
    type: ratio
    type_params:
      numerator: approved_claim_count
      denominator: claim_count
```

---

**dbt CI/CD in GitHub Actions:**
```yaml
# .github/workflows/dbt_ci.yml
name: dbt CI

on:
  pull_request:
    branches: [main]
    paths: ['models/**', 'tests/**', 'macros/**']

jobs:
  dbt-ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install dbt
        run: pip install dbt-snowflake==1.7.0

      - name: dbt deps
        run: dbt deps

      - name: dbt compile (syntax check)
        run: dbt compile --target ci

      - name: dbt run (changed models only)
        run: |
          dbt run \
            --select state:modified+ \    # changed models + downstream
            --defer \                      # use prod state for unmodified models
            --state ./prod-state \         # production manifest for comparison
            --target ci

      - name: dbt test (changed models)
        run: |
          dbt test \
            --select state:modified+ \
            --defer \
            --state ./prod-state \
            --target ci

      - name: dbt source freshness
        run: dbt source freshness --target ci
```

### Common Mistakes
- ❌ No incremental lookback — `WHERE date >= MAX(date)` misses late-arriving data
- ❌ Using `append` strategy for mutable data — creates duplicate rows on retry
- ❌ `unique_key` as composite without `merge_update_columns` — updates ALL columns (including audit cols)
- ❌ No `dbt test` in CI — merging breaking schema changes to prod silently
- ❌ `ref()` instead of `source()` for raw tables — bypasses source freshness tracking
- ✅ 3-day lookback on incremental models: `WHERE date >= MAX(date) - 3 DAYS`
- ✅ Use `state:modified+` in CI to test only what changed + downstream
- ✅ `dbt compile` first in CI — catches Jinja/SQL syntax errors without running

### Interview Questions
1. What are the four dbt incremental strategies and when do you use each?
2. What is the `is_incremental()` macro and how does it control full vs incremental runs?
3. What is a dbt exposure and why is it useful for governance?
4. How do you implement CI/CD for dbt and only test changed models?
5. What is the dbt Semantic Layer and how does it differ from a traditional BI semantic layer?
6. What is a dbt macro and give a production use case?
7. What is `defer` in dbt CI and why does it reduce CI run time?

---

## 14.2 Data Contracts

### Theory

**What is a data contract?**
A formal, machine-readable agreement between a data producer and its consumers defining:
- Schema (field names, types, required/optional)
- Semantics (what each field means)
- SLAs (freshness, availability, quality)
- Versioning rules (what changes are breaking vs non-breaking)
- Ownership (who is responsible)

**Why data contracts matter:**
```
Without contracts:
  Producer changes column name → 15 downstream pipelines break
  Producer adds NOT NULL → existing NULLs cause load failures
  Producer changes date format → silent wrong data in downstream

With contracts:
  Any change validated against contract → breaking changes caught in CI
  Consumers declare what they need → producers know impact of changes
  Schema Registry enforces Avro contracts at runtime
```

**Breaking vs non-breaking changes:**
```
NON-BREAKING (safe to deploy):
  ✅ Add new optional field (with default value)
  ✅ Widen a type (INT → BIGINT)
  ✅ Relax a constraint (NOT NULL → nullable)
  ✅ Add a new enum value

BREAKING (requires consumer migration):
  ❌ Rename a field
  ❌ Remove a field
  ❌ Narrow a type (BIGINT → INT)
  ❌ Add a required field without default
  ❌ Change field semantics (amount: dollars → cents)
```

**Data contract specification (OpenDataContract standard):**
```yaml
# data-contract.yaml
dataContractSpecification: 0.9.3
id: claims-silver-v1
info:
  title: Silver Claims Dataset
  version: 2.1.0
  description: Cleansed and validated claims data
  owner: Claims Data Engineering
  contact:
    name: Malay Kumar Padhi
    email: de-team@company.com

servers:
  production:
    type: snowflake
    account: company.snowflake.com
    database: CLAIMS_DW
    schema: SILVER

terms:
  usage: Internal analytics and ML only
  limitations: No PII sharing outside approved use cases
  billing: Charged to Claims Analytics cost center
  noticePeriod: P3M    # 3 months notice for breaking changes

models:
  silver_claims:
    description: One row per validated claim transaction
    type: table
    fields:
      claim_id:
        type: varchar
        required: true
        unique: true
        description: Business key from source system
        pii: false
      patient_id:
        type: varchar
        required: true
        description: Patient identifier (foreign key to dim_patient)
        pii: true
        classification: phi   # HIPAA Protected Health Information
      billed_amount:
        type: decimal(12,2)
        required: true
        minimum: 0
        maximum: 10000000
        description: Amount billed to payer in USD
      claim_date:
        type: date
        required: true
        description: Date of service
      status_code:
        type: varchar
        required: true
        enum: [SUBMITTED, APPROVED, DENIED, PENDING, APPEALED]

quality:
  - type: sql
    description: No duplicate claim_ids
    query: |
      SELECT COUNT(*) AS duplicates
      FROM silver_claims
      GROUP BY claim_id HAVING COUNT(*) > 1
    mustBe: 0

  - type: sql
    description: Freshness — data loaded within 6 hours
    query: |
      SELECT DATEDIFF('hour', MAX(ingestion_ts), CURRENT_TIMESTAMP())
      FROM silver_claims
    mustBeLessThan: 6

servicelevels:
  availability: 99.5%
  freshness:
    description: Data available by 6 AM UTC daily
    threshold: 6h
  completeness:
    description: At least 95% of source claims present
    threshold: 95%
```

**Contract testing in CI pipeline:**
```python
# Using Soda Core for contract validation
# soda/checks/silver_claims.yml
checks for silver_claims:
  - row_count > 0
  - missing_count(claim_id) = 0
  - duplicate_count(claim_id) = 0
  - min(billed_amount) >= 0
  - invalid_count(status_code) = 0:
      valid values: [SUBMITTED, APPROVED, DENIED, PENDING, APPEALED]
  - freshness(ingestion_ts) < 6h

# Run in Airflow after load
soda_check = BashOperator(
    task_id='soda_contract_check',
    bash_command='soda scan -d snowflake -c soda/configuration.yml soda/checks/silver_claims.yml',
)

# Using Great Expectations
validator.expect_column_values_to_be_in_set(
    "status_code",
    ["SUBMITTED", "APPROVED", "DENIED", "PENDING", "APPEALED"]
)
```

**Schema Registry as runtime contract enforcement:**
```python
# Avro schema = the contract (enforced at produce/consume time)
claims_schema = {
    "type": "record",
    "name": "ClaimSubmitted",
    "namespace": "com.company.claims",
    "fields": [
        {"name": "claim_id",      "type": "string"},
        {"name": "patient_id",    "type": "string"},
        {"name": "billed_amount", "type": {"type": "bytes", "logicalType": "decimal",
                                           "precision": 12, "scale": 2}},
        {"name": "claim_date",    "type": {"type": "int", "logicalType": "date"}},
        # Adding field WITH default = non-breaking (backward compatible)
        {"name": "diagnosis_code","type": ["null", "string"], "default": null}
    ]
}

# Schema Registry compatibility modes:
# BACKWARD:  new schema can read old data (add fields with defaults)
# FORWARD:   old schema can read new data (delete fields from new)
# FULL:      both backward + forward
# NONE:      no compatibility check (dangerous in production)

# Set compatibility per subject
curl -X PUT http://schema-registry:8081/config/claims-submitted-value \
  -d '{"compatibility": "FULL"}'
```

### Tradeoffs
| Approach | Enforcement | Overhead | Best For |
|---|---|---|---|
| Schema Registry (Avro) | Runtime (reject incompatible produce) | Low (serialization only) | Kafka streaming |
| dbt tests | Post-load (detect in pipeline) | Medium | Batch SQL transforms |
| OpenDataContract YAML | CI/CD (prevent merge) | Low (YAML validation) | Documentation + governance |
| Soda / Great Expectations | Post-load + scheduled | Medium | DQ + contract validation |
| JSON Schema | API-level (REST) | Low | API data contracts |

### Common Mistakes
- ❌ Data contracts as documentation only (not enforced) — violated without consequence
- ❌ No versioning strategy — v1 → v2 without migration plan breaks consumers
- ❌ Contract owned by producer only — consumers must co-define what they need
- ❌ Only schema contract, no SLA contract — data arrives on time but garbage quality
- ✅ Contract enforcement in CI/CD: PRs that break contracts fail before merge
- ✅ Semantic versioning: MAJOR.MINOR.PATCH — breaking.non-breaking.fix

### Interview Questions
1. What is a data contract and what does it contain beyond a schema definition?
2. What is the difference between a breaking and non-breaking schema change?
3. How does Schema Registry enforce contracts at runtime in Kafka pipelines?
4. How do you implement contract testing in a dbt + Airflow pipeline?
5. What is the OpenDataContract standard?
6. How do data contracts relate to Data Mesh's "data as a product" principle?

---

## 14.3 Quick Reference — One-Page Decision Guide

### Architecture decisions
```
New cloud DWH project:
  SQL-first, BI-heavy      → Snowflake
  GCP-native, serverless   → BigQuery
  ML-heavy, Python-first   → Databricks
  Azure + Power BI         → Microsoft Fabric

Open table format:
  Databricks primary       → Delta Lake
  Multi-engine / multi-cloud → Iceberg
  CDC / high-freq upserts  → Hudi MOR

File format:
  Analytics / S3 lake      → Parquet (Snappy/Zstd)
  Kafka messages           → Avro + Schema Registry
  Hive ACID                → ORC

Streaming:
  Complex stateful, large scale → Flink
  Kafka-native, simple topology → Kafka Streams
  Spark-already-invested        → Spark Structured Streaming
  Simple, low-volume            → Spark micro-batch

Orchestration:
  General enterprise       → Airflow (MWAA / Astronomer)
  Asset-centric, ML-heavy  → Dagster
  Python-native            → Prefect
  dbt-only pipelines       → dbt Cloud

CDC:
  Zero source impact, < 100ms lag → Debezium (log-based)
  Simple, no log access           → Query-based (timestamp)
  Snowflake native                → Snowflake Streams + Tasks
```

### Performance quick fixes
```
Slow Snowflake query:
  1. Query Profile → check partitions scanned (pruning?)
  2. Check spillage to remote storage → scale up VW
  3. Check result cache hit → was data changed?
  4. Add clustering on filter columns
  5. Rewrite function on column (YEAR(date) → BETWEEN)
  6. Create materialized view for repeated aggregation

Slow Spark job:
  1. Spark UI → find slow stage → find skewed tasks
  2. Enable AQE: spark.sql.adaptive.enabled=true
  3. Check shuffle size → reduce with broadcast join
  4. Check GC time > 10% → increase executor memory
  5. Check partitions: too many small → coalesce; too few → repartition
  6. Python UDFs? → rewrite as native Spark SQL functions

BigQuery expensive query:
  1. Preview bytes scanned before running
  2. Add partition filter (WHERE claim_date = ...)
  3. SELECT specific columns (not SELECT *)
  4. Add clustering on filter columns
  5. Materialize repeated subquery as materialized view
```

### SCD type selection
```
Attribute never changes:                    Type 0
Error correction, history irrelevant:       Type 1
Full history required (standard):           Type 2 ← default choice
One lookback period only:                   Type 3
Fast current lookups + full history:        Type 4
High-velocity volatile attributes:          Type 5 (mini-dim)
Both historical + current needed per row:   Type 6
```

### Fact table type selection
```
Individual event (claim, transaction):      Transaction fact
State at regular intervals (balance):       Periodic snapshot
Lifecycle tracking (claim → paid):          Accumulating snapshot
Event occurrence, no measures:              Factless fact
```

### Join algorithm selection (Spark)
```
One table < 10MB:                           Broadcast join (force with hint)
Both tables large, equi-join:               Hash join (default)
Pre-sorted data or range join:              Sort-merge join
Severe skew on join key:                    Salting + AQE
Both tables bucketed on join key:           Bucket join (no shuffle)
```

### Kafka producer settings
```
Highest durability (financial):
  acks=all, min.insync.replicas=2, enable.idempotence=true,
  retries=MAX_INT, max.in.flight=5

Highest throughput (logs):
  acks=1, compression.type=lz4, linger.ms=5, batch.size=65536

Exactly-once:
  enable.idempotence=true, transactional.id=<unique-id>,
  acks=all + read_committed on consumer
```

### dbt model materialization guide
```
Raw/staging views (fast iteration):         view
Intermediate transforms:                    ephemeral or view
Large fact tables (incremental load):       incremental (merge strategy)
Small dimension tables:                     table (full refresh)
Expensive repeated aggregations:            materialized view
SCD Type 2 dimensions:                      snapshot
```

### Data quality check placement
```
Bronze (ingest boundary):    Schema validation only (tolerate source issues)
Bronze → Silver:             Full DQ: nulls, types, duplicates, validity
Silver → Gold:               Business rules, referential integrity, aggregation checks
Gold (output):               Row count ±5%, sum reconciliation vs upstream
```

---

## 14.4 Study Plan — 4 Weeks to Principal Architect

### Week 1: Foundations + Storage + Modeling
```
Day 1-2: Chunk 01 — DB/DWH/Lake, OLTP/OLAP, CAP, Distributed Systems
Day 3-4: Chunk 02 — Parquet/ORC/Avro, Delta/Iceberg/Hudi, Partitioning
Day 5-6: Chunk 03 — ER Modeling, Dimensional, Star/Snowflake, Fact types
Day 7:   Chunk 04 — SCDs 0-7, Data Vault 2.0, Surrogate keys
Practice: Draw healthcare bus matrix from memory. Write SCD Type 2 MERGE SQL.
```

### Week 2: Architecture + Processing
```
Day 8-9:  Chunk 05 — Medallion, Lambda/Kappa, Data Mesh, Multi-cloud
Day 10-11: Chunk 06 — ETL/ELT, CDC internals, Exactly-once, Idempotency
Day 12-13: Chunk 13 — Spark internals, Kafka deep dive, Streaming windows
Day 14:   Chunk 07 — Airflow internals, Scheduling, Failure recovery
Practice: Debug a Spark job using Spark UI. Write a Debezium connector config.
          Draw Lambda vs Kappa vs Lakehouse architecture from memory.
```

### Week 3: Cloud + Performance + Governance
```
Day 15-16: Chunk 08 — Snowflake, BigQuery, Databricks, Azure Synapse
Day 17-18: Chunk 09 — Query optimization, Join strategies, Cost optimization
Day 19-20: Chunk 10 — RBAC/ABAC, Encryption, Lineage, DQ frameworks
Day 21:   Chunk 14 — dbt advanced, Data contracts, Quick reference
Practice: Run EXPLAIN on a slow query. Write row access policy. Run dbt snapshot.
```

### Week 4: Production Mastery + Interview Prep
```
Day 22-23: Chunk 11 — Production problems (late data, schema drift, DR)
Day 24-25: Chunk 12 — Interview master: scenarios, architecture design
Day 26-27: Quick Reference revision + flashcard creation
Day 28:   Mock interview: answer 5 scenario questions out loud (record yourself)
           Review "Principal vs Senior" checklist
```

### Role-specific reading paths
```
Targeting Snowflake Architect role:
  Priority: Chunks 01, 03, 04, 08, 09, 10, 12
  Focus: Micro-partitions, clustering, streams/tasks, masking, RLS, Time Travel

Targeting Databricks/Spark Engineer:
  Priority: Chunks 02, 06, 08, 09, 13, 14
  Focus: Delta Lake internals, AQE, skew, streaming, dbt incremental

Targeting Data Modeler/Architect:
  Priority: Chunks 03, 04, 05, 10, 14
  Focus: DV2 deep dive, SCD types, bus matrix, data contracts

Targeting Data Platform/Principal Architect:
  Priority: ALL chunks
  Focus: Chunk 12 scenarios + Chapter 14 quick reference
  Differentiator: Tradeoff articulation + "why this over that"

Targeting Healthcare/BFSI DE role:
  Priority: Chunks 03, 04, 08, 10, 11
  Focus: HIPAA masking, PHI governance, claims modeling, audit lineage
```

### Final week interview checklist
```
□ Can explain every SCD type with a real example
□ Can draw Medallion architecture + DV2 architecture from memory
□ Can debug a slow Snowflake query step by step
□ Can explain Kafka ISR + acks=all + min.insync.replicas together
□ Can articulate: "Why dbt over stored procs?"
□ Can articulate: "Why DV2 over Kimball for integration layer?"
□ Can articulate: "Why Iceberg over Delta for multi-cloud?"
□ Can design a fraud detection streaming pipeline end-to-end
□ Can answer: "Your numbers don't match — walk me through debugging"
□ Can explain exactly-once semantics at Kafka + Flink + sink level
□ Know your own GitHub projects (HealthVault360-CortexAI) cold
□ STAR format ready for 3 production war stories
```

---

*DE Master Doc — Complete. 14 Chunks. 7,500+ lines.*
*Built by Malay Kumar Padhi | linkedin.com/in/mkpadhi | github.com/Techy-Malay*
