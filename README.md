# 🏗 Data Engineering Principal Architect Guide

> A Principal Architect-level Data Engineering reference — 14 phases, 60+ topics, production code, real tradeoffs, and interview Q&A.

Built by **Malay Kumar Padhi** · Senior Data Architect · 17 Years Experience
Specializations: Snowflake · Data Vault 2.0 · dbt · Informatica IICS · Healthcare & BFSI

[![LinkedIn](https://img.shields.io/badge/LinkedIn-mkpadhi-blue?logo=linkedin&style=flat-square)](https://linkedin.com/in/mkpadhi)
[![GitHub](https://img.shields.io/badge/GitHub-Techy--Malay-black?logo=github&style=flat-square)](https://github.com/Techy-Malay)
[![SnowPro](https://img.shields.io/badge/SnowPro-Associate-29B5E8?logo=snowflake&style=flat-square)](https://linkedin.com/in/mkpadhi)
![Lines](https://img.shields.io/badge/Lines-7%2C665%2B-gold?style=flat-square)
![Phases](https://img.shields.io/badge/Phases-14-teal?style=flat-square)
![Topics](https://img.shields.io/badge/Topics-60%2B-green?style=flat-square)

---

## 📌 What This Is

This is not a glossary. This is not a cheat sheet.

This is an **architect-level production reference** built for:
- Senior → Principal / Staff Data Engineer interview preparation at IBM, Capgemini, Coforge, and FAANG
- Day-to-day production decision-making on real enterprise stacks
- Data Vault Mastery course curriculum backbone
- Principal Architect promotion preparation

Every topic follows a consistent **6-layer format**:

```
1. Theory              — Internals, mental model, how it actually works
2. Production Example  — Real code (Healthcare / BFSI context)
3. Tradeoffs           — Comparison tables, decision criteria
4. Common Mistakes     — What kills prod pipelines and what kills interviews
5. Interview Questions — Questions actually asked at Principal Architect level
6. Architecture Diagram — ASCII diagrams, SQL / Python / YAML snippets
```

---

## 📂 Repository Structure

```
de-principal-architect-guide/
│
├── README.md
│
├── Phases/
│   ├── 01_fundamentals.md
│   ├── 02_storage_formats.md
│   ├── 03_data_modeling_p1.md
│   ├── 04_data_modeling_p2.md
│   ├── 05_modern_architectures.md
│   ├── 06_processing.md
│   ├── 07_orchestration.md
│   ├── 08_cloud_de.md
│   ├── 09_performance.md
│   ├── 10_governance.md
│   ├── 11_production.md
│   ├── 12_interview_master.md
│   ├── 13_spark_kafka.md
│   └── 14_dbt_contracts_quickref.md
│
└── combined/
    ├── de_master_doc_full.md        ← Phases 01–12 combined
    └── de_master_doc_v2_full.md     ← All 14 Phases (latest · 7,665 lines)
```

---

## 📚 Content Map — All 14 Phases

| # | File | Key Topics | Lines |
|---|---|---|---|
| 01 | [01_fundamentals.md](Phases/01_fundamentals.md) | DB vs DWH vs Lake vs Lakehouse · OLTP vs OLAP · CAP Theorem · Distributed Systems | ~253 |
| 02 | [02_storage_formats.md](Phases/02_storage_formats.md) | Parquet vs ORC vs Avro · Delta vs Iceberg vs Hudi · Partitioning · Clustering · Z-ordering · Compression · Small File Problem | ~392 |
| 03 | [03_data_modeling_p1.md](Phases/03_data_modeling_p1.md) | ER Modeling CDM/LDM/PDM · Normalization 1NF–BCNF · Kimball Dimensional · Star/Snowflake · Bus Matrix · Fact Table Types · Special Dimensions | ~465 |
| 04 | [04_data_modeling_p2.md](Phases/04_data_modeling_p2.md) | SCD Types 0–7 (complete) · Data Vault 2.0 Hub/Link/Sat/PIT/Bridge · Anchor Modeling · Surrogate vs Natural vs Hash Keys | ~508 |
| 05 | [05_modern_architectures.md](Phases/05_modern_architectures.md) | Medallion Architecture · Lambda/Kappa · Data Mesh · Data Fabric · Microsoft Fabric · Multi-Cloud | ~439 |
| 06 | [06_processing.md](Phases/06_processing.md) | ETL vs ELT · Batch vs Streaming · CDC Internals (Debezium) · Exactly-Once · Idempotency · Retry · Backpressure · Event-Driven | ~715 |
| 07 | [07_orchestration.md](Phases/07_orchestration.md) | DAG concepts · Airflow internals · Scheduling · XCom · Dependency management · Failure recovery · Backfill · Airflow vs Dagster vs Prefect | ~531 |
| 08 | [08_cloud_de.md](Phases/08_cloud_de.md) | Snowflake deep architecture · BigQuery internals · Databricks Photon/DLT/MLflow · AWS/Azure/GCP comparison · Synapse · Fabric | ~524 |
| 09 | [09_performance.md](Phases/09_performance.md) | Query optimization · Join strategies · Indexing · Predicate pushdown · Caching · Statistics · Cost optimization | ~600 |
| 10 | [10_governance.md](Phases/10_governance.md) | RBAC/ABAC · Row/column security · Encryption HIPAA/GDPR/PCI · Lineage (OpenLineage) · Metadata mgmt · DQ frameworks | ~543 |
| 11 | [11_production.md](Phases/11_production.md) | Late-arriving data · Duplicate handling · Schema drift · Reprocessing · Multi-tenant design · Disaster recovery RPO/RTO | ~640 |
| 12 | [12_interview_master.md](Phases/12_interview_master.md) | 4 full architecture scenarios · "Why this over that" · Hands-on labs · Principal vs Senior checklist | ~766 |
| 13 | [13_spark_kafka.md](Phases/13_spark_kafka.md) | Spark Job/Stage/Task · Shuffle mechanics · Memory model · Tungsten/AQE · Kafka log/compaction/ISR · Transactions · MirrorMaker 2 · Streaming Windows | ~569 |
| 14 | [14_dbt_contracts_quickref.md](Phases/14_dbt_contracts_quickref.md) | dbt incremental strategies · Macros/packages · Exposures · Semantic Layer · dbt CI/CD · Data Contracts · Quick Reference · 4-Week Study Plan | ~720 |

**Total: 7,665+ lines · 293 KB · 60+ topics · 200+ interview questions**

---

## 🎯 Who This Is For

| Role | Recommended Path |
|---|---|
| **Senior DE → Principal/Staff** | All 14 phases end-to-end. Focus on tradeoffs. |
| **Interview prep (FAANG / Big Consulting)** | Phase 12 first → drill weak areas → Phase 14 Quick Reference |
| **Architect making tech decisions** | Jump to relevant phase via content map |
| **Data Engineering course student** | Phases 01 → 14 sequentially |
| **Healthcare / BFSI specialist** | Phases 03, 04, 08, 10, 11 (all examples use claims/patient domain) |

---

## 🗓 4-Week Study Plan

| Week | Phases | Daily Focus |
|---|---|---|
| **Week 1** | 01 → 04 | Foundations + Storage + ER Modeling + DV2 + SCDs |
| **Week 2** | 05 → 07 + 13 | Architectures + CDC + Streaming + Airflow + Spark/Kafka deep dive |
| **Week 3** | 08 → 10 + 14 | Cloud DE + Performance + Governance + dbt advanced |
| **Week 4** | 11 → 12 + revise | Production problems + Interview scenarios + Quick Reference |

**Role-specific shortcuts:**

```
Snowflake Architect   → Phases 01, 03, 04, 08, 09, 10, 12
Databricks / Spark    → Phases 02, 06, 08, 09, 13, 14
Data Modeler          → Phases 03, 04, 05, 10, 14
Healthcare / BFSI DE  → Phases 03, 04, 08, 10, 11
Full Principal Arch   → All 14 phases (end with Phase 14 Quick Reference)
```

---

## 🛠 Tech Stack Covered

| Category | Technologies |
|---|---|
| Cloud DWH | Snowflake · BigQuery · Redshift · Azure Synapse |
| Lakehouse | Delta Lake · Apache Iceberg · Apache Hudi |
| Processing | Apache Spark · Apache Flink · dbt Core · Informatica IICS |
| Orchestration | Apache Airflow · Dagster · Prefect · dbt Cloud |
| Streaming | Apache Kafka · Kinesis · Event Hubs · Debezium · MirrorMaker 2 |
| Modeling | erwin · SqlDBM · dbt snapshots · Data Vault 2.0 |
| Governance | Collibra DGC · Apache Atlas · Microsoft Purview · Unity Catalog |
| Cloud | AWS (EMR · Glue · MWAA · DMS) · Azure (ADF · Fabric · Synapse) · GCP (Dataproc · Dataflow · BigQuery) |
| File Formats | Parquet · ORC · Avro · Delta · Iceberg · Hudi |
| DQ / Contracts | Great Expectations · dbt tests · Monte Carlo · Soda · OpenDataContract |

---

## 🏥 Domain Context

All production examples are drawn from 17 years of enterprise delivery:
- **Healthcare:** Claims processing (837/835), patient 360, HIPAA governance, payer/provider analytics
- **BFSI:** Fraud detection, regulatory reporting, trade lifecycle, customer 360, AML

---

## 📈 Related Projects

| Project | Description | Stack |
|---|---|---|
| [HealthVault360-CortexAI](https://github.com/Techy-Malay/HealthVault360-CortexAI) | Snowflake Cortex AI — RAG pipeline, Patient 360, HIPAA governance | Snowflake · Cortex · Streamlit |

---

## 🚀 Quick Start

```bash
git clone https://github.com/Techy-Malay/de-principal-architect-guide.git
cd de-principal-architect-guide

# Full combined document (all 14 phases)
open combined/de_master_doc_v2_full.md

# Start with interview prep
open Phases/12_interview_master.md

# DV2 + SCD deep dive
open Phases/04_data_modeling_p2.md

# Quick reference + study plan
open Phases/14_dbt_contracts_quickref.md
```

---

## 📬 Connect

| | |
|---|---|
| LinkedIn | [linkedin.com/in/mkpadhi](https://linkedin.com/in/mkpadhi) |
| GitHub | [github.com/Techy-Malay](https://github.com/Techy-Malay) |
| WhatsApp | [+91 9922992309](https://wa.me/919922992309) |

---

## ⭐ Star This Repo

If this helped you — give it a ⭐. It helps other Data Engineers and Architects find it.

Found an error or want to contribute an example from your domain? Open a PR.

---

*v2.0 · May 2026 · Maintained by Malay Kumar Padhi · 17 YOE · Snowflake · DV2 · dbt · Healthcare & BFSI*
