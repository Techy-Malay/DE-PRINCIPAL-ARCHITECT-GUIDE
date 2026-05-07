# DE Master Doc — Chunk 07: Orchestration
> Format: Theory → Production Example → Tradeoffs → Common Mistakes → Interview Questions

---

## 7.1 DAG Concepts

### Theory

**DAG (Directed Acyclic Graph):** A graph of tasks where edges represent dependencies and no cycles exist. The foundational concept behind all workflow orchestrators.

**Key properties:**
- **Directed:** Dependencies flow in one direction (A → B means B depends on A)
- **Acyclic:** No circular dependencies (A → B → C → A is invalid — would loop forever)
- **Graph:** Multiple paths, parallel branches, fan-in, fan-out supported

**DAG components:**
- **Node/Task:** A unit of work (run SQL, trigger Spark job, call API, send email)
- **Edge/Dependency:** Defines execution order between tasks
- **Root node:** Task with no upstream dependencies (entry point)
- **Leaf node:** Task with no downstream dependencies (exit point)

**DAG execution patterns:**
```
Linear:          A → B → C → D

Fan-out:         A → B
                 A → C
                 A → D

Fan-in:          A → D
                 B → D
                 C → D

Diamond:         A → B → D
                 A → C → D

Complex:         A → B → E → G
                 A → C → F → G
                 A → D     → G
```

**Critical Path:** The longest path through the DAG. Determines minimum possible completion time. Optimizing the critical path = optimizing overall DAG runtime.

### Why DAGs (not pipelines or cron)?
```
Cron:      Fixed schedule, no dependency awareness, no retry, no monitoring
Pipeline:  Linear only, no fan-out parallelism
DAG:       Dependency-aware, parallel where possible, retry per task,
           monitoring per task, backfill, SLA alerting
```

---

## 7.2 Apache Airflow — Internals

### Theory

**Airflow components:**

| Component | Role |
|---|---|
| **Webserver** | Flask UI — DAG visualization, task logs, trigger, monitor |
| **Scheduler** | Parses DAG files, schedules task instances, sends to executor |
| **Executor** | Executes tasks. Pluggable: Local, Celery, Kubernetes, Sequential |
| **Metadata DB** | PostgreSQL/MySQL — stores DAG runs, task instances, connections, variables |
| **Worker** | (Celery/K8s) Processes executing tasks |
| **DAG Directory** | Python files defining DAGs. Scheduler parses continuously. |

**Airflow execution flow:**
```
1. Scheduler parses DAG files (every 30s by default)
2. Scheduler creates DagRun for each scheduled interval
3. Scheduler checks task dependencies → queues ready tasks
4. Executor picks up queued tasks → sends to workers
5. Workers execute tasks → report status to metadata DB
6. Scheduler updates task instance state
7. Webserver reads metadata DB → displays status
```

**Task states:**
```
none → scheduled → queued → running → success
                                    → failed → up_for_retry → queued (retry)
                                    → failed (max retries) → skipped
```

**Executors:**

| Executor | Parallelism | Setup | Best For |
|---|---|---|---|
| Sequential | 1 task at a time | None | Dev/testing only |
| Local | N tasks (multi-process) | Simple | Single-node, small scale |
| Celery | N tasks across N workers | Redis/RabbitMQ broker | Mid-scale production |
| Kubernetes | N tasks in K8s pods | K8s cluster | Large-scale, isolation |
| CeleryKubernetes | Hybrid | Complex | Mixed workloads |

### Airflow DAG — Production Example
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,          # each run independent
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=60),
    'email_on_failure': True,
    'email': ['de-alerts@company.com'],
    'sla': timedelta(hours=4),         # alert if not done in 4h
}

with DAG(
    dag_id='claims_daily_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',     # 2 AM UTC daily
    start_date=days_ago(1),
    catchup=False,                     # don't backfill missed runs
    max_active_runs=1,                 # no concurrent runs
    tags=['claims', 'production'],
) as dag:

    # Task 1: Extract from source
    extract_claims = PythonOperator(
        task_id='extract_claims_from_oracle',
        python_callable=extract_claims_fn,
        op_kwargs={'run_date': '{{ ds }}'},   # templated date
    )

    # Task 2a: Load raw to S3 (parallel with 2b)
    load_to_s3 = PythonOperator(
        task_id='load_raw_to_s3',
        python_callable=load_s3_fn,
    )

    # Task 2b: Validate schema (parallel with 2a)
    validate_schema = PythonOperator(
        task_id='validate_source_schema',
        python_callable=validate_fn,
    )

    # Task 3: Spark transform (after both 2a and 2b)
    spark_transform = EmrAddStepsOperator(
        task_id='spark_silver_transform',
        job_flow_id='{{ var.value.emr_cluster_id }}',
        steps=[{
            'Name': 'SilverTransform',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', '--py-files', 's3://code/transform.py']
            }
        }],
    )

    # Task 4: Load to Snowflake
    load_snowflake = SnowflakeOperator(
        task_id='load_to_snowflake_gold',
        snowflake_conn_id='snowflake_prod',
        sql='CALL sp_load_fact_claims(\'{{ ds }}\');',
    )

    # Task 5: dbt run
    dbt_run = BashOperator(
        task_id='dbt_mart_refresh',
        bash_command='dbt run --select marts.claims --target prod',
    )

    # Task 6: Data quality check
    dq_check = PythonOperator(
        task_id='great_expectations_dq',
        python_callable=run_ge_checkpoint,
    )

    # Dependency graph
    extract_claims >> [load_to_s3, validate_schema]  # fan-out
    [load_to_s3, validate_schema] >> spark_transform  # fan-in
    spark_transform >> load_snowflake >> dbt_run >> dq_check
```

---

## 7.3 Scheduling Patterns

### Theory

**Cron expressions in Airflow:**
```
┌─── minute (0-59)
│  ┌─── hour (0-23)
│  │  ┌─── day of month (1-31)
│  │  │  ┌─── month (1-12)
│  │  │  │  ┌─── day of week (0-6, 0=Sunday)
│  │  │  │  │
*  *  *  *  *

Examples:
0 2 * * *        = 2:00 AM daily
0 6 * * 1        = 6:00 AM every Monday
*/15 * * * *     = every 15 minutes
0 0 1 * *        = midnight on 1st of every month
0 8,12,18 * * *  = 8 AM, 12 PM, 6 PM daily
```

**Airflow-specific schedule strings:**
```python
schedule_interval='@daily'    # midnight UTC
schedule_interval='@hourly'   # top of every hour
schedule_interval='@weekly'   # midnight Sunday
schedule_interval=None        # manual trigger only
schedule_interval=timedelta(hours=6)  # every 6 hours
```

**Data interval concept (Airflow 2.2+):**
Airflow runs AFTER the interval completes. `schedule='@daily'` with `start_date=2024-01-01` runs at 2024-01-02 00:00 to process 2024-01-01's data.

```
Logical date (data_interval_start): 2024-01-01
Execution date (actual run):        2024-01-02 00:00
Data interval:                      2024-01-01 00:00 → 2024-01-02 00:00

Template variables:
  {{ ds }}           = 2024-01-01 (logical date, YYYY-MM-DD)
  {{ data_interval_start }} = 2024-01-01T00:00:00
  {{ data_interval_end }}   = 2024-01-02T00:00:00
```

**Event-based triggering (Airflow 2.0+):**
```python
# TriggerDagRunOperator: DAG A triggers DAG B on completion
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

trigger_downstream = TriggerDagRunOperator(
    task_id='trigger_reporting_dag',
    trigger_dag_id='claims_reporting_pipeline',
    wait_for_completion=True,           # wait for triggered DAG
    conf={'run_date': '{{ ds }}'},      # pass context
)

# Dataset-based scheduling (Airflow 2.4+)
from airflow.datasets import Dataset

claims_dataset = Dataset("s3://datalake/silver/claims/")

# Producer DAG marks dataset as updated
with DAG(..., schedule='@daily') as producer_dag:
    load_task = PythonOperator(...)
    load_task.outlets = [claims_dataset]   # marks dataset updated

# Consumer DAG triggers when dataset is updated
with DAG(..., schedule=[claims_dataset]) as consumer_dag:
    ...  # runs automatically when claims_dataset is updated
```

---

## 7.4 Dependency Management

### Theory

**Task-level dependencies:**
```python
# Bitshift operator (most common)
task_a >> task_b >> task_c           # linear
task_a >> [task_b, task_c]           # fan-out
[task_b, task_c] >> task_d           # fan-in

# set_upstream / set_downstream (equivalent)
task_b.set_upstream(task_a)
task_c.set_downstream(task_d)
```

**Cross-DAG dependencies:**

**Pattern 1: ExternalTaskSensor**
```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_extract_dag',
    external_dag_id='extract_pipeline',
    external_task_id='final_task',
    allowed_states=['success'],
    execution_delta=timedelta(hours=1),    # upstream runs 1h earlier
    timeout=3600,                           # fail after 1 hour waiting
    poke_interval=60,                       # check every 60s
    mode='reschedule',                      # release worker slot while waiting
)
```

**Pattern 2: Dataset-based (Airflow 2.4+)** ← preferred
```python
# No polling. Event-driven. DAG runs when upstream marks dataset.
with DAG(schedule=[upstream_dataset]) as dag:
    ...
```

**Trigger rules (dependency logic):**
```python
from airflow.utils.trigger_rule import TriggerRule

# Default: all_success — run only if ALL upstream tasks succeeded
task = PythonOperator(trigger_rule=TriggerRule.ALL_SUCCESS, ...)

# all_done — run regardless of upstream success/failure
cleanup = PythonOperator(trigger_rule=TriggerRule.ALL_DONE, ...)

# one_success — run if ANY upstream task succeeded
# one_failed — run if ANY upstream task failed (alerting)
# none_failed — run if no upstream tasks failed (skipped OK)

# Common pattern: cleanup always runs, alert on failure
alert_task = PythonOperator(
    task_id='send_failure_alert',
    trigger_rule=TriggerRule.ONE_FAILED,
    python_callable=send_slack_alert,
)
[task_a, task_b, task_c] >> alert_task
```

**XCom (Cross-Communication between tasks):**
```python
# Push value from task
def extract_fn(**context):
    record_count = run_extract()
    context['ti'].xcom_push(key='record_count', value=record_count)

# Pull value in downstream task
def validate_fn(**context):
    count = context['ti'].xcom_pull(
        task_ids='extract_claims',
        key='record_count'
    )
    if count == 0:
        raise ValueError("No records extracted — aborting pipeline")
```

**XCom limitations:**
- Stored in Airflow metadata DB → not for large data
- Max size: ~48KB (SQLite), larger with PostgreSQL but still limited
- For large data: write to S3/GCS, pass the path via XCom

---

## 7.5 Failure Recovery

### Theory

**Failure types and recovery strategies:**

| Failure Type | Example | Recovery |
|---|---|---|
| Transient failure | Network timeout, API rate limit | Retry with backoff |
| Task failure | Bad data, assertion error | Fix + clear task + re-run |
| DAG failure | Dependency never satisfied | Investigate upstream |
| Infra failure | Worker node crash | Re-queue task automatically |
| Data failure | Wrong data in target | Backfill from correct source |

**Airflow recovery mechanisms:**

**1. Task-level retry**
```python
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
}
```

**2. Clear and re-run**
```bash
# Clear specific task instance (reset to None state → re-run)
airflow tasks clear claims_daily_pipeline \
    --task-id spark_silver_transform \
    --start-date 2024-01-15 \
    --end-date 2024-01-15

# Clear entire DAG run
airflow dags clear claims_daily_pipeline \
    --start-date 2024-01-15
```

**3. Backfill (catch-up missed runs)**
```bash
# Run DAG for missed historical dates
airflow dags backfill claims_daily_pipeline \
    --start-date 2024-01-01 \
    --end-date 2024-01-10

# Important: pipeline must be idempotent for backfill to be safe
# Re-running 2024-01-05 must produce same result as original run
```

**4. SLA Miss alerting**
```python
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    send_pagerduty_alert(f"SLA missed: {dag.dag_id}")

with DAG(
    ...,
    sla_miss_callback=sla_miss_callback,
    default_args={'sla': timedelta(hours=6)},
) as dag:
    ...
```

**5. Dead letter / failed task notification**
```python
def failure_callback(context):
    task_instance = context['task_instance']
    send_slack_message(
        channel='#data-alerts',
        message=f"❌ Task failed: {task_instance.dag_id}.{task_instance.task_id}\n"
                f"Date: {context['ds']}\n"
                f"Log: {task_instance.log_url}"
    )

default_args = {
    'on_failure_callback': failure_callback,
}
```

**Idempotency requirement for failure recovery:**
```
Backfill and retry ONLY work correctly if pipeline is idempotent.
Running the same DAG for 2024-01-05 twice must produce identical output.

Idempotency patterns:
  • MERGE instead of INSERT (upsert, no duplicates on retry)
  • Partition overwrite: spark.write.mode("overwrite").partitionBy("date")
  • Truncate + reload per partition (DELETE WHERE date=X, then INSERT)
  • dbt incremental with unique_key (deduplicates on re-run)
```

### Common Airflow Mistakes
- ❌ `catchup=True` on a new DAG with old `start_date` — triggers hundreds of backfill runs immediately
- ❌ Long-running tasks in the Airflow scheduler process — scheduler should only orchestrate, not process
- ❌ Large data in XCom — metadata DB bloat, slow Airflow UI
- ❌ Dynamic task generation with complex Python in DAG file — scheduler parses every 30s, slow parsing = scheduler lag
- ❌ No SLA defined — silent delays go undetected for hours
- ❌ `depends_on_past=True` without understanding implication — one failure blocks all future runs for that DAG
- ✅ Keep DAG files lightweight — no DB queries, no heavy imports at module level
- ✅ Use `catchup=False` for new production DAGs
- ✅ Set `max_active_runs=1` to prevent concurrent runs on daily pipelines
- ✅ `mode='reschedule'` on sensors — releases worker slot while waiting (vs `mode='poke'` which holds it)

### Interview Questions — Full Set

**DAG + Airflow Architecture:**
1. What components make up Apache Airflow and what does each do?
2. How does the Airflow scheduler decide when to queue a task?
3. What is the difference between CeleryExecutor and KubernetesExecutor?
4. What happens when the Airflow metadata DB goes down?

**Scheduling:**
5. What is the difference between `execution_date` and `data_interval_start` in Airflow 2.x?
6. How does Airflow's `catchup=True` work and when is it dangerous?
7. What is dataset-based scheduling in Airflow 2.4+ and how does it differ from ExternalTaskSensor?

**Dependencies:**
8. What trigger rules does Airflow support and when would you use `ONE_FAILED`?
9. What is XCom and what are its limitations?
10. How do you implement cross-DAG dependencies in Airflow?

**Failure Recovery:**
11. What is the difference between clearing a task and re-triggering a DAG?
12. How do you backfill missed DAG runs and what prerequisite must your pipeline meet?
13. What is `depends_on_past` and when would enabling it cause problems?
14. How do you implement SLA alerting in Airflow?

**Production:**
15. What is the small scheduler problem in Airflow and how do you fix it?
16. Why should DAG files not contain DB connections or heavy imports at module level?
17. How does `mode='reschedule'` on sensors improve Airflow scalability vs `mode='poke'`?

---

## 7.6 Airflow vs Alternatives

### Comparison
| Dimension | Airflow | Prefect | Dagster | dbt Cloud |
|---|---|---|---|---|
| Primary use | General orchestration | Python-native workflows | Data-aware orchestration | dbt-specific |
| DAG definition | Python (decorator/class) | Python (flow/task) | Python (assets/ops) | YAML + SQL |
| Data awareness | Limited | Limited | First-class (assets) | dbt models |
| UI quality | Functional | Modern | Excellent | Good |
| Dynamic DAGs | Complex | Native | Native | Limited |
| Cloud managed | MWAA, Astronomer, GCC | Prefect Cloud | Dagster Cloud | dbt Cloud |
| Learning curve | High | Medium | High | Low (SQL) |
| Best for | Enterprise, complex deps | Python-heavy teams | Asset-centric workflows | dbt shops |

**Dagster's Software-Defined Assets (SDA) — the modern approach:**
```python
# Dagster: define assets (not tasks)
from dagster import asset, AssetIn

@asset
def bronze_claims():
    """Raw claims data from source"""
    return extract_and_land_raw()

@asset(ins={"bronze": AssetIn("bronze_claims")})
def silver_claims(bronze):
    """Cleansed claims data"""
    return apply_dq_rules(bronze)

@asset(ins={"silver": AssetIn("silver_claims")})
def gold_claims_mart(silver):
    """Business-ready claims mart"""
    return build_star_schema(silver)

# Dagster tracks lineage automatically
# Re-materialize any asset independently
# Observe data freshness per asset
```

### Interview Questions
1. When would you choose Dagster over Airflow?
2. What is a Software-Defined Asset in Dagster and how does it differ from an Airflow task?
3. What is the difference between Prefect flows and Airflow DAGs?
4. How does dbt Cloud scheduling compare to Airflow for dbt-only pipelines?

---

*Next: Chunk 08 — Cloud Data Engineering (Snowflake deep dive, BigQuery internals, Databricks architecture, Synapse/Fabric, AWS/Azure/GCP comparison)*
