# Airflow Practice

Learning-focused Apache Airflow repository built to practice DAG design patterns beyond basic tutorials: branching, trigger rules, conditional execution, and the TaskFlow API — plus a containerized mock API that one DAG ingests from incrementally.

## What's in here

| DAG | Demonstrates |
|---|---|
| `dags/myfirst_dag.py` | Baseline DAG structure |
| `dags/pipeline_layout.py` | Multi-task pipeline layout and dependency wiring |
| `dags/taskflow_api.py` | Airflow's TaskFlow API (`@task` decorators vs. classic operators) |
| `dags/incremental_api_data_load.py` | Incremental ingestion from an external API, with branching/trigger-rule logic for conditional paths |
| `dags/test_dag.py` | Scratch DAG for testing scheduler/trigger behavior |

`api_code/` is a small containerized FastAPI service (own `Dockerfile` + `docker-compose.yaml`) that `incremental_api_data_load.py` pulls from — it exists so the incremental-load DAG has a real (if mock) upstream source to ingest from incrementally, rather than reading a static local file.

## Running it

```bash
# 1. Start the mock API
cd airflow/api_code
docker compose up -d

# 2. Start Airflow (webserver + scheduler + Postgres)
cd ..
docker compose -f docker-compose.yaml up -d
```

Airflow UI: http://localhost:8080. `airflow/.env` only sets `AIRFLOW_UID` (standard Airflow docker-compose requirement — no secrets in this file).

## Why this exists

Most "Airflow practice" repos stop at a hello-world DAG. This one is built around one non-trivial question: how do you incrementally pull from an API inside Airflow without re-processing data you've already ingested, and how do you branch a DAG based on what that pull returns? `incremental_api_data_load.py`, together with the trigger-rule/branching logic in `pipeline_layout.py`, is the answer.

## Next steps
- [ ] Add a `tests/` folder with DAG-validation tests (e.g. `dagbag.import_errors == {}`)
- [ ] Add a GitHub Actions workflow to lint/validate DAGs on push
