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
