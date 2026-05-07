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
