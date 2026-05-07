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
