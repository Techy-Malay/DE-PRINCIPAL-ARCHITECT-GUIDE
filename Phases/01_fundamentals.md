# DE Master Doc — Chunk 01: Fundamentals
> Format per topic: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 1.1 Database vs DWH vs Data Lake vs Lakehouse

### Theory
| System | Storage Model | Schema | ACID | Primary Workload |
|---|---|---|---|---|
| OLTP Database | Row-store | Write-time | Full | High-freq reads/writes (ms) |
| Data Warehouse | Column-store | Write-time | Partial | Analytical queries (seconds-min) |
| Data Lake | Object storage | Read-time | None | ML, raw exploration |
| Lakehouse | Column on object store | Both | Full (via format) | BI + ML unified |

**OLTP Database:** Normalized to 3NF/BCNF. B-tree indexes for single-row lookup. WAL for durability. Examples: PostgreSQL, MySQL, Oracle. Designed for thousands of transactions/second.

**Data Warehouse:** Column-store enables vectorized execution — reads only queried columns. Compression ratios 5-10x vs row-store. Denormalized (star schema). Examples: Snowflake, Redshift, BigQuery. Designed for scanning billions of rows on a few columns.

**Data Lake:** Cheap object storage (S3, ADLS, GCS). Raw data in native format. Schema applied at read time — risk: "data swamps" (data without governance). Best for ML features, unstructured data, data science exploration.

**Lakehouse:** Applies DWH-grade governance on top of lake storage via open table formats (Delta Lake, Iceberg, Hudi). One copy of data serves BI + ML + streaming. Eliminates two-tier ETL complexity. ACID on S3.

### Architecture Diagram
```
OLTP Sources (Oracle/Postgres)
     │
     ├──[ETL]──► Data Warehouse (Snowflake) ──► BI / Reports
     │
     ├──[raw]──► Data Lake (S3/ADLS) ──► ML / Data Science
     │
     └──[unified]──► LAKEHOUSE (Delta/Iceberg)
                         ├── Bronze (Raw)
                         ├── Silver (Cleansed)
                         └── Gold (Business) ──► BI + ML + API
```

### Production Example (BFSI)
- Core banking → **Oracle 19c** (OLTP, strict ACID)
- Daily regulatory batch → **Snowflake DWH** via Informatica IICS
- Raw logs, clickstream, ML features → **S3 Data Lake** (Parquet)
- Fraud ML + regulatory reporting unified → **Delta Lake on Databricks** (Lakehouse)

### Tradeoffs
| Dimension | Database | DWH | Lake | Lakehouse |
|---|---|---|---|---|
| Cost | Medium | High | Low | Low-Medium |
| ACID | Full | Partial | None | Full |
| ML Support | Poor | Limited | Excellent | Excellent |
| BI Performance | Poor | Excellent | Moderate | Excellent |
| Schema Governance | Strong | Strong | Weak | Strong |

### Common Mistakes
- ❌ Running analytical queries on OLTP — causes row lock contention, kills transaction throughput
- ❌ Treating a Data Lake as a DWH substitute — without governance it becomes a swamp
- ❌ Building a Lakehouse without metadata catalog (Glue/Unity Catalog) — you get lake chaos
- ✅ Migration path: Start DWH → add Lake for ML → consolidate into Lakehouse

### Interview Questions
1. Why would you choose Lakehouse over maintaining separate Lake + DWH?
2. What is schema-on-read and what governance risks does it introduce?
3. How does Delta Lake achieve ACID transactions on top of S3?
4. When would you NOT consolidate into a Lakehouse? (Answer: regulatory data isolation, legacy BI dependency, Snowflake-only shop)

---

## 1.2 OLTP vs OLAP

### Theory

**OLTP (Online Transaction Processing)**
- Storage: Row-store. Full row read on every access.
- Access pattern: Single-row lookup via primary key / index.
- Optimization: B-tree indexes, buffer pool, WAL.
- Metric: Transactions per second (TPS).
- Normalization: 3NF — minimize write amplification.

**OLAP (Online Analytical Processing)**
- Storage: Column-store. Only queried columns read from disk.
- Access pattern: Full-column scans, GROUP BY, aggregations.
- Optimization: Vectorized execution, compression (RLE, dictionary), predicate pushdown.
- Metric: Query latency on billions of rows.
- Normalization: Denormalized (star schema) — minimize JOINs.

**Why column-store beats row-store for analytics:**
Scanning 1B rows for 3 columns — column-store reads only those 3 columns. Row-store reads entire rows (all 50+ columns). Column values compress far better (similar values adjacent): a `status` column with 3 values compresses 100x via RLE.

**HTAP (Hybrid Transactional/Analytical Processing):**
Single engine for both workloads. Examples: TiDB, CockroachDB, SingleStore. Eliminates ETL latency for real-time analytics. Trade-off: neither best-in-class OLTP nor OLAP performance. Right for: fraud detection on live transactions, live dashboards on operational data.

### Production SQL Example
```sql
-- OLTP: claims transaction (row-store, milliseconds)
INSERT INTO claims (claim_id, patient_id, amount, status)
VALUES ('CLM-2024-001', 'PAT-123', 1500.00, 'SUBMITTED');

-- OLAP: aggregation (column-store, seconds on billions of rows)
SELECT payer_name, diagnosis_code,
       SUM(amount)           AS total_claims,
       AVG(processing_days)  AS avg_tat
FROM   fact_claims
JOIN   dim_payer USING (payer_key)
WHERE  claim_year = 2024
GROUP  BY 1, 2
ORDER  BY total_claims DESC;
```

### Tradeoffs
| Dimension | OLTP | OLAP | HTAP |
|---|---|---|---|
| Storage | Row | Column | Both |
| Write performance | Excellent | Poor | Good |
| Scan performance | Poor | Excellent | Good |
| Data freshness | Real-time | T-1/hours | Real-time |
| Cost | Medium | Medium-High | High |
| Normalization | 3NF | Star/Snowflake | Both |

### Common Mistakes
- ❌ Running OLAP queries on OLTP — full table scan + lock contention brings down production
- ❌ Choosing HTAP thinking it will match dedicated OLTP/OLAP — it won't; it's a compromise
- ❌ Storing pre-aggregated results in OLTP tables to speed up dashboards — use a proper DWH
- ✅ Read replicas bridge the gap for low-complexity analytical needs on OLTP data

### Interview Questions
1. Why does column compression improve OLAP performance beyond just IO reduction?
2. What is vectorized execution and why does it matter for analytical engines?
3. When would you justify HTAP over separate OLTP + DWH?
4. Explain why an analytical query on OLTP causes row-level lock contention.

---

## 1.3 CAP Theorem

### Theory
A distributed system can guarantee **at most 2 of 3** simultaneously:

- **C — Consistency:** Every read returns the most recent write or an error. All nodes see identical data.
- **A — Availability:** Every request receives a response (not necessarily latest data). System never rejects.
- **P — Partition Tolerance:** System operates despite network partitions (nodes can't communicate).

**Critical insight:** In real distributed systems, network partitions are inevitable — P is non-negotiable. The real choice is **C vs A** when a partition occurs.

**CP Systems:** HBase, ZooKeeper, MongoDB (default). Return error during partition. Right for: financial balances, inventory, booking systems.

**AP Systems:** Cassandra, DynamoDB (default), CouchDB. Return stale data during partition. Right for: social feeds, shopping carts, DNS resolution.

**PACELC extension (more realistic):**
Even **without partition (E=else)**, there's a Latency (L) vs Consistency (C) trade-off. Most real systems tune this dynamically (Cassandra: `ONE` vs `QUORUM` vs `ALL`).

### Production Example
```
BFSI — Account Balance → CP required
  Bank must be consistent. $500 withdrawal must reflect everywhere.
  Cassandra (AP) is wrong here — stale read → double spend.
  Use: PostgreSQL (CP), Google Spanner (external consistency CP).

E-commerce — Shopping Cart → AP acceptable
  Amazon's cart can show stale items during partition.
  Dynamo paper (2007): user experience (availability) > stale cart.
  Use: DynamoDB (AP, eventual consistency).
```

### Tradeoffs
| System | CAP Choice | When to use |
|---|---|---|
| PostgreSQL | CP | Financial data, inventory |
| Cassandra | AP (tunable) | IoT, time-series, high write |
| MongoDB | CP (default) | Documents, flexible schema |
| DynamoDB | AP (default) | High-scale, cart, session |
| Spanner | CP (global) | Global financial, multi-region consistent |
| ZooKeeper | CP | Leader election, config management |

### Common Mistakes
- ❌ Treating CAP as static — Cassandra's consistency is tunable per operation
- ❌ Ignoring PACELC — latency/consistency trade-off exists even without failures
- ❌ Using AP system (Cassandra) for financial transactions without compensating transactions
- ❌ Applying CAP to Snowflake/BigQuery — CAP applies to distributed transactional stores, not analytical engines
- ✅ Configure Cassandra at `QUORUM` read+write for stronger consistency at cost of availability

### Interview Questions
1. Why is P always required in a real distributed system?
2. How does Cassandra let you tune between C and A?
3. Where does Snowflake sit in the CAP triangle and why?
4. Explain eventual consistency with a concrete example.
5. What is the PACELC model and why is it more practical than CAP?

---

## 1.4 Distributed Systems Basics

### Theory

**Sharding (Horizontal Partitioning)**
Data split across nodes. Each shard holds a subset.

| Strategy | How | Pro | Con |
|---|---|---|---|
| Range sharding | A-M → Shard 1, N-Z → Shard 2 | Simple, range queries efficient | Hot spots on sequential keys |
| Hash sharding | `shard = hash(key) % N` | Uniform distribution | Range queries hit all shards |
| Consistent hashing | Virtual ring, nodes claim ranges | Add/remove nodes = minimal reshuffling | More complex |

**Replication**
Copies of data on multiple nodes for fault tolerance and read scaling.

- **Synchronous:** Write acknowledged only after all replicas confirm. Strong consistency, higher latency.
- **Asynchronous:** Write acknowledged immediately, replicas catch up. Low latency, risk of data loss on failure.
- **Semi-synchronous:** Acknowledge after ≥1 replica (MySQL default).

**Consensus (Raft / Paxos)**
Algorithm for distributed nodes to agree on a single value. Raft requires majority quorum (n/2 + 1). Used in: etcd (Kubernetes), CockroachDB, TiKV. Critical for leader election and log replication.

**Coordinator Patterns**
- **Master-slave:** Single writer. Simple, no conflict resolution. Single point of failure.
- **Multi-master:** Multiple writers. Requires conflict resolution (last-write-wins, CRDTs).
- **Leaderless (Dynamo-style):** Any node accepts writes. Version vectors or CRDTs for conflict resolution.

### Production Example — Snowflake's Distributed Design
```
Snowflake = "Shared-Disk" architecture (NOT shared-nothing):
  - Storage layer: S3 (shared across all compute nodes)
  - Compute layer: Virtual Warehouses (ephemeral, no data ownership)
  - Coordination: Cloud Services Layer (metadata, query optimization)

Implication: Multiple VWs can read same data simultaneously (no
contention). VW adds nodes = faster scan, not more data access.
Local SSD on each VW node caches remote S3 reads → performance.
```

**Kafka's partition model (sharding + replication):**
```
Topic → N partitions (shards, horizontal scale)
Each partition → 1 leader + N-1 followers (replication)
Leader handles all reads/writes for that partition
ISR (In-Sync Replicas): set of replicas caught up to leader
acks=all: wait for all ISR replicas → no data loss guarantee
```

### Common Mistakes
- ❌ Hash sharding on time-series data — queries like "last 7 days" hit all shards
- ❌ Using synchronous replication across geographically distant nodes — latency kills throughput
- ❌ Single master with no failover — SPOF in production
- ✅ Consistent hashing is why Cassandra can add nodes with minimal data reshuffling (virtual nodes/vnodes)

### Interview Questions
1. What is consistent hashing and why does it minimize reshuffling when nodes are added?
2. Explain how Snowflake achieves compute/storage separation and its trade-offs.
3. What is a split-brain scenario and how does Raft prevent it?
4. When would you choose leaderless replication over leader-based?
5. What is the difference between ISR and OSR in Kafka and why does it matter for `acks=all`?

---

*Next: Chunk 02 — Storage & File Formats (Parquet/ORC/Avro, Delta/Iceberg/Hudi, Partitioning/Clustering/Z-order, Compression, Small File Problem)*
