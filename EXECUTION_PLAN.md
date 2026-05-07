# DE Principal Architect Guide — Execution Plan
> From files on disk → GitHub repo → LinkedIn visibility → Interview-ready

---

## Phase 0 — GitHub Setup
**Time: 20 minutes · Do this TODAY**

```bash
# 1. Create repo on GitHub (2 min)
#    github.com → New repository
#    Name: de-principal-architect-guide
#    Visibility: Public
#    DO NOT initialize with README (you have your own)

# 2. Create folder structure locally (3 min)
mkdir de-principal-architect-guide
cd de-principal-architect-guide
mkdir Phases combined

# 3. Copy your files (2 min)
# Drop all 14 .md files into Phases/
# Rename them (remove chunk_ prefix — already done per your screenshot):
#   01_fundamentals.md ✓
#   02_storage_formats.md ✓
#   ...
#   14_dbt_contracts_quickref.md ✓

# Drop combined docs into combined/
#   de_master_doc_full.md
#   de_master_doc_v2_full.md

# Drop README.md at root

# 4. Initialize and push (5 min)
git init
git add .
git commit -m "feat: DE Principal Architect Guide v1.0 — 14 phases, 7665 lines"
git branch -M main
git remote add origin https://github.com/Techy-Malay/de-principal-architect-guide.git
git push -u origin main

# 5. Pin repo on GitHub profile (3 min)
#    github.com/Techy-Malay → Customize profile → Pin this repo
#    Add repo description: "Principal Architect-level DE reference — 14 domains, 7665 lines"
#    Add topics: data-engineering snowflake data-vault dbt kafka spark airflow
```

**Verify:** Open GitHub URL → confirm folder structure matches README links

---

## Phase 1 — LinkedIn Launch Post
**Time: 15 minutes · Do within 24 hours of GitHub push**

```
Action items:
  □ Use the LinkedIn post draft below (separate deliverable)
  □ Post between 8–9 AM IST Tuesday or Wednesday (peak DE audience)
  □ Add repo link in first comment (not in post body — better reach)
  □ Tag 3 people who would find it useful
  □ Reply to every comment within the first 2 hours (boosts algorithm)

Expected outcome:
  500–2000 impressions in 48 hours
  10–50 repo stars in first week
  2–5 recruiter messages (IBM/Capgemini/Coforge target)
```

---

## Phase 2 — Study Plan Execution
**Time: 4 weeks · 1–2 hours/day**

### Week 1 — Foundations + Modeling (Days 1–7)
**Daily time: 90 minutes**

| Day | Phase File | Practice Task | Time |
|---|---|---|---|
| Mon | 01_fundamentals.md | Draw DB/DWH/Lake/Lakehouse arch from memory | 90 min |
| Tue | 02_storage_formats.md | Write Parquet vs Iceberg vs Hudi decision table without notes | 90 min |
| Wed | 03_data_modeling_p1.md | Design healthcare bus matrix from scratch (claims, eligibility, encounters) | 90 min |
| Thu | 04_data_modeling_p2.md | Write SCD Type 2 MERGE SQL for dim_patient. Draw DV2 Hub/Link/Sat structure | 90 min |
| Fri | 01–04 revision | Answer all "Interview Questions" sections out loud. Time yourself. | 60 min |
| Sat | Hands-on | Create dim_patient + fact_claims DDL in Snowflake trial account | 2 hrs |
| Sun | Rest / light review | Re-read Phase 14 Quick Reference decision tables | 30 min |

**Week 1 milestone:** Can explain SCD Types 0–7, draw DV2 structure, and design a star schema from scratch without notes.

---

### Week 2 — Architectures + Processing + Orchestration (Days 8–14)
**Daily time: 90 minutes**

| Day | Phase File | Practice Task | Time |
|---|---|---|---|
| Mon | 05_modern_architectures.md | Draw Medallion + Lambda/Kappa + Data Mesh from memory | 90 min |
| Tue | 06_processing.md | Write Debezium connector config. Explain exactly-once end-to-end | 90 min |
| Wed | 13_spark_kafka.md | Explain Spark Job/Stage/Task to a colleague. Write AQE config | 90 min |
| Thu | 07_orchestration.md | Write a 5-task Airflow DAG with fan-out, fan-in, retry, SLA | 90 min |
| Fri | 05–07 + 13 revision | Answer scenario: "Design a real-time fraud detection pipeline" | 60 min |
| Sat | Hands-on | Set up local Kafka + Debezium using docker-compose from Phase 12 lab | 2 hrs |
| Sun | Rest / light review | Re-read Phase 12 "Why this over that" section | 30 min |

**Week 2 milestone:** Can design Lambda/Kappa/Lakehouse trade-offs. Can explain Kafka ISR + acks=all. Can write a production Airflow DAG.

---

### Week 3 — Cloud + Performance + Governance (Days 15–21)
**Daily time: 90 minutes**

| Day | Phase File | Practice Task | Time |
|---|---|---|---|
| Mon | 08_cloud_de.md | Explain Snowflake 3-tier arch. Compare BigQuery vs Snowflake pricing model | 90 min |
| Tue | 09_performance.md | Debug a slow query: write 5 diagnostic questions you ask first | 90 min |
| Wed | 10_governance.md | Write Snowflake Row Access Policy for multi-tenant payer isolation | 90 min |
| Thu | 14_dbt_contracts_quickref.md | Write dbt incremental model with merge strategy + 3-day lookback | 90 min |
| Fri | 08–10 + 14 revision | Answer: "Your query takes 45 minutes — walk me through debugging it" | 60 min |
| Sat | Hands-on | Run dbt snapshot on a dim table. Check `dbt docs generate` lineage graph | 2 hrs |
| Sun | Rest / light review | Re-read Phase 09 performance quick fixes section | 30 min |

**Week 3 milestone:** Can tune a Snowflake query using Query Profile. Can implement HIPAA masking. Can write a production dbt incremental model.

---

### Week 4 — Production + Interview Mastery (Days 22–28)
**Daily time: 2 hours**

| Day | Phase File | Practice Task | Time |
|---|---|---|---|
| Mon | 11_production.md | Explain late-arriving data handling. Write a reprocessing checklist | 2 hrs |
| Tue | 12_interview_master.md | Answer all 4 architecture scenarios out loud. Record yourself. | 2 hrs |
| Wed | Full revision | Go through Phase 14 Quick Reference — test yourself on every decision table | 2 hrs |
| Thu | Mock interview | Ask a colleague to run 5 questions from Phase 12. Debrief. | 2 hrs |
| Fri | War stories | Write 3 STAR-format production war stories (situation, task, action, result) | 2 hrs |
| Sat | GitHub polish | Add 2–3 more stars, README improvements, respond to any GitHub issues | 1 hr |
| Sun | Final checklist | Run through Phase 12 "Principal vs Senior" checklist. Mark gaps. | 1 hr |

**Week 4 milestone:** Can answer all 5 Principal Architect interview questions cold. GitHub repo has 20+ stars.

---

## Phase 3 — Hands-on Lab Projects
**Time: 2–4 weeks after study plan · Weekends only**

| Lab | File Reference | Estimated Time | Outcome |
|---|---|---|---|
| Lab 1: Snowflake Claims DWH | Phase 12, Lab 1 | 4 hours | Working DDL, masking, RLS, clustering |
| Lab 2: dbt Claims Models | Phase 12, Lab 2 | 6 hours | Full dbt project with staging, marts, snapshots, tests |
| Lab 3: Kafka + Debezium CDC | Phase 12, Lab 3 | 4 hours | Docker-compose CDC pipeline producing to Kafka |
| Lab 4: Spark + Delta Pipeline | Phase 12, Lab 4 | 4 hours | Bronze→Silver→Gold pipeline with MERGE + OPTIMIZE |
| Lab 5: Airflow + DQ | Phase 12, Lab 5 | 4 hours | DAG with Great Expectations checkpoint + DLQ |

**Add each lab as a subfolder in the repo:**
```
de-principal-architect-guide/
└── labs/
    ├── lab01_snowflake_claims_dwh/
    ├── lab02_dbt_claims_models/
    ├── lab03_kafka_debezium_cdc/
    ├── lab04_spark_delta_pipeline/
    └── lab05_airflow_dq_pipeline/
```

---

## Phase 4 — Interactive HTML LMS Version
**Time: 1 weekend (8–10 hours) · After labs are done**

Convert this MD doc into the dark navy/gold LMS format (3-column, tabbed, dark mode default).

```
Deliverable: de_master_doc_lms.html
Features:
  □ 14-phase navigation (top nav chips)
  □ Per-question 6-tab format (Theory/Production/Tradeoffs/Mistakes/IQ/Diagram)
  □ Per-question reveal timer
  □ Left sidebar: all 60+ topics, search filter
  □ Right sidebar: progress, stats, bookmarks
  □ Dark/Light/Contrast themes
  □ Zoom controls
  □ WhatsApp CTA + LinkedIn/GitHub author block

Use case: Your Data Vault Mastery course student gift
          Interview prep tool to share with DE community
```

---

## Phase 5 — Course Integration
**Time: Ongoing · Batch 1 launch timeline**

```
Week 1 of course:   Phases 01, 02 (Foundations + Storage)
Week 2 of course:   Phases 03, 04 (Modeling — core course content)
Week 3 of course:   Phases 05, 06 (Architecture + Processing)
Bonus sessions:     Phases 08, 12 (Cloud DE + Interview Master)
Student gift:       de_master_doc_lms.html (HTML LMS version)
```

---

## Phase 6 — Version 2 Content (3 months out)
**Time: 1 chunk per month · New session each**

| Chunk | Topic | Priority |
|---|---|---|
| 15 | dbt + Snowflake end-to-end project walkthrough | 🔴 High |
| 16 | Real-time ML Feature Store design | 🟡 Medium |
| 17 | FinOps for data teams (cost management deep dive) | 🟡 Medium |
| 18 | Interview war stories (STAR format, 5 real scenarios) | 🔴 High |

---

## Summary Timeline

| Phase | Action | Time Required | Target Date |
|---|---|---|---|
| 0 | GitHub push + pin | 20 min | **Today** |
| 1 | LinkedIn launch post | 15 min | **Tomorrow morning** |
| 2 | Study plan Week 1 | 7 × 90 min | Week 1 |
| 2 | Study plan Week 2 | 7 × 90 min | Week 2 |
| 2 | Study plan Week 3 | 7 × 90 min | Week 3 |
| 2 | Study plan Week 4 | 7 × 2 hrs | Week 4 |
| 3 | Hands-on labs | 5 × 4–6 hrs | Weeks 5–6 (weekends) |
| 4 | HTML LMS version | 1 weekend | Week 7 |
| 5 | Course integration | Ongoing | Batch 1 launch |
| 6 | V2 content (Chunks 15–18) | 1 per month | Month 3–6 |

---

*Built by Malay Kumar Padhi · linkedin.com/in/mkpadhi · github.com/Techy-Malay*
