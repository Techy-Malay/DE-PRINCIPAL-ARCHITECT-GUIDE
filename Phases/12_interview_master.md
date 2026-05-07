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
