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
