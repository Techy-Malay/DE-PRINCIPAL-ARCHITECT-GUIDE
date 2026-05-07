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
