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
