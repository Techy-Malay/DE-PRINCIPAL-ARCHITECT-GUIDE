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
