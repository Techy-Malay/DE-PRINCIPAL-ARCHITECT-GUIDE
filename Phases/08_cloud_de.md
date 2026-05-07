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
