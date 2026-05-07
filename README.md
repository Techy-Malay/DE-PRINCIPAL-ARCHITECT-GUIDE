# 🏗 Data Engineering Principal Architect Guide

> A Principal Architect-level Data Engineering reference — 12 domains, 55+ topics, production examples, tradeoffs, and interview Q&A.

Built by **Malay Kumar Padhi** · Senior Data Architect · 17 Years Experience  
Specializations: Snowflake · Data Vault 2.0 · dbt · Informatica IICS · Healthcare & BFSI

[![LinkedIn](https://img.shields.io/badge/LinkedIn-mkpadhi-blue?logo=linkedin)](https://linkedin.com/in/mkpadhi)
[![GitHub](https://img.shields.io/badge/GitHub-Techy--Malay-black?logo=github)](https://github.com/Techy-Malay)

---

## 📌 What This Is

This is not a glossary. This is not a cheat sheet.

This is an **architect-level reference** built for:
- Senior Data Engineer / Architect interviews at IBM, Capgemini, Coforge, and Big Tech
- Day-to-day production decision-making
- Principal / Staff Engineer promotion preparation
- Data Engineering course curriculum backbone

Every topic follows a consistent 6-layer format:

```
1. Theory              — How it works, internals, mental model
2. Production Example  — Real code, real stack, real domain (Healthcare / BFSI)
3. Tradeoffs           — Comparison tables, decision criteria
4. Common Mistakes     — What kills prod pipelines and interviews
5. Interview Questions — Questions you will actually be asked
6. Architecture Diagram — ASCII diagrams, SQL/Python/YAML snippets
```

---

## 📂 Structure

```
de-principal-architect-guide/
├── README.md
├── chunks/
│   ├── 01_fundamentals.md
│   ├── 02_storage_formats.md
│   ├── 03_data_modeling_p1.md
│   ├── 04_data_modeling_p2_scds_dv2.md
│   ├── 05_modern_architectures.md
│   ├── 06_processing_cdc_streaming.md
│   ├── 07_orchestration_airflow.md
│   ├── 08_cloud_de_snowflake_bigquery.md
│   ├── 09_performance_optimization.md
│   ├── 10_governance_security.md
│   ├── 11_production_problems.md
│   └── 12_interview_master.md
└── combined/
    └── de_master_doc_full.md
```

---

## 📚 Content Map

| # | Chunk | Key Topics |
|---|---|---|
| 01 | [Fundamentals](chunks/01_fundamentals.md) | DB vs DWH vs Lake vs Lakehouse · OLTP vs OLAP · CAP Theorem · Distributed Systems |
| 02 | [Storage & File Formats](chunks/02_storage_formats.md) | Parquet vs ORC vs Avro · Delta/Iceberg/Hudi · Partitioning · Clustering · Z-order · Compression · Small File Problem |
| 03 | [Data Modeling Part 1](chunks/03_data_modeling_p1.md) | ER Modeling · CDM/LDM/PDM · Normalization 1NF–BCNF · Dimensional Modeling · Star/Snowflake · Bus Matrix · Fact Table Types · Special Dimensions |
| 04 | [Data Modeling Part 2](chunks/04_data_modeling_p2_scds_dv2.md) | SCD Types 0–7 · Data Vault 2.0 · Anchor Modeling · Surrogate vs Natural Keys · Hash Keys |
| 05 | [Modern Architectures](chunks/05_modern_architectures.md) | Medallion · Lambda/Kappa · Data Mesh · Data Fabric · Microsoft Fabric · Multi-Cloud |
| 06 | [Processing](chunks/06_processing_cdc_streaming.md) | ETL vs ELT · Batch vs Streaming · CDC Internals · Event-Driven · Exactly-Once · Idempotency · Retry · Backpressure |
| 07 | [Orchestration](chunks/07_orchestration_airflow.md) | DAG Concepts · Airflow Internals · Scheduling · Dependency Management · Failure Recovery |
| 08 | [Cloud Data Engineering](chunks/08_cloud_de_snowflake_bigquery.md) | Snowflake Architecture · BigQuery Internals · Databricks · Synapse · AWS/Azure/GCP Comparison |
| 09 | [Performance Optimization](chunks/09_performance_optimization.md) | Query Optimization · Join Strategies · Indexing · Predicate Pushdown · Caching · Statistics · Cost Optimization |
| 10 | [Governance & Security](chunks/10_governance_security.md) | RBAC/ABAC · Row/Column Security · Encryption · Lineage · Metadata Management · Data Quality Frameworks |
| 11 | [Production Problems](chunks/11_production_problems.md) | Late-Arriving Data · Duplicate Handling · Schema Drift · Reprocessing · Multi-Tenant Design · Disaster Recovery |
| 12 | [Interview Master](chunks/12_interview_master.md) | Scenario Questions · Tradeoff Analysis · Architecture Design · "Why this over that?" · Real Enterprise Examples |

---

## 🎯 Who This Is For

| Role | How to Use |
|---|---|
| **Senior DE preparing for Principal/Staff** | Read end-to-end. Focus on tradeoffs and "why this over that" sections. |
| **Interviewing at FAANG / Big Consulting** | Start with Chunk 12 (Interview Master), then drill into weak areas. |
| **Architect making tech decisions** | Use as a reference. Jump to the relevant chunk. |
| **Data Engineering course student** | Follow chunks 01→12 sequentially. |

---

## 🧠 How to Use This Guide

**For interview prep (4-week plan):**

```
Week 1: Chunks 01, 02, 03  — Foundations + Storage + Modeling basics
Week 2: Chunks 04, 05      — DV2 + SCDs + Modern Architectures
Week 3: Chunks 06, 07, 08  — Processing + Orchestration + Cloud
Week 4: Chunks 09–12       — Performance + Governance + Production + Interview
```

**For quick reference:**
- Use the Content Map above to jump to the relevant chunk
- Each chunk is self-contained — no need to read sequentially

**For revision before an interview:**
- Read Chunk 12 (Interview Master) first
- Then read the "Interview Questions" section at the bottom of each topic

---

## 🛠 Tech Stack Covered

| Category | Technologies |
|---|---|
| Cloud DWH | Snowflake · BigQuery · Redshift · Synapse |
| Lakehouse | Delta Lake · Apache Iceberg · Apache Hudi |
| Processing | Apache Spark · Apache Flink · dbt · Informatica IICS |
| Orchestration | Apache Airflow · Dagster · Prefect |
| Streaming | Apache Kafka · Kinesis · Event Hubs · Debezium |
| Modeling Tools | erwin · SqlDBM · dbt snapshots |
| Governance | Collibra DGC · Apache Atlas · Microsoft Purview |
| Cloud Platforms | AWS · Azure · GCP |
| Storage Formats | Parquet · ORC · Avro · Delta · Iceberg |

---

## 🏥 Domain Context

Production examples in this guide draw from:
- **Healthcare:** Claims processing, patient 360, HIPAA governance, payer/provider analytics
- **BFSI:** Fraud detection, regulatory reporting, trade lifecycle, customer 360

These are real patterns from 17 years of enterprise delivery — not textbook examples.

---

## 📈 Related Projects

| Project | Description |
|---|---|
| [HealthVault360-CortexAI](https://github.com/Techy-Malay/HealthVault360-CortexAI) | Snowflake Cortex AI portfolio project — RAG pipeline, Patient 360, HIPAA governance |

---

## 📬 Connect

| Platform | Link |
|---|---|
| LinkedIn | [linkedin.com/in/mkpadhi](https://linkedin.com/in/mkpadhi) |
| GitHub | [github.com/Techy-Malay](https://github.com/Techy-Malay) |
| WhatsApp | [+91 9922992309](https://wa.me/919922992309) |

---

## ⭐ If This Helped You

Give this repo a ⭐ — it helps other Data Engineers find it.

Open a PR if you spot an error or want to contribute an example from your domain.

---

*Last updated: 2026 · Maintained by Malay Kumar Padhi*
