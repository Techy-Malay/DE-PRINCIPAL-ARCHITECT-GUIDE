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
