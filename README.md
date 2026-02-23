# 🚀 Fleet SaaS GTM Analytics & AI Reverse ETL Pipeline

An end-to-end Modern Data Stack project designed to unify marketing and sales data, establish a Single Source of Truth (SSOT) for revenue attribution, and operationalize AI-driven lead scoring via Reverse ETL.



## 📌 Executive Summary
This project simulates a production-grade data environment for a B2B SaaS company. It moves beyond standard dashboards by integrating **Defensive Data Engineering** and **Applied AI**. The pipeline ingests messy CRM/Marketing data, strictly tests and transforms it using dbt, and feeds the cleaned profiles into a local Large Language Model (Llama 3.1) to generate actionable sales hooks that are pushed back into the data warehouse.

## 🏗 Architecture & Engineering Highlights

1. **Idempotent Extract & Load (Python/Docker):** * Custom Python scripts generate mock B2B Go-To-Market data (HubSpot web visits, Salesforce opportunities) using the `Faker` library.
   * Data is safely and idempotently loaded into a pristine `RAW` Snowflake schema.
2. **Defensive Transformation (dbt):**
   * **Staging Layer:** Acts as a data firewall utilizing `try_cast()`, `trim()`, and `coalesce()` to handle corrupted API dates and null identifiers gracefully without crashing the pipeline.
   * **Mart Layer:** Joins marketing touchpoints with sales pipeline data to build `dim_accounts` and `fct_revenue_attribution`.
   
   

3. **Data Governance & CI/CD:**
   * **Automated CI/CD:** GitHub Actions workflow automatically runs `black`, `flake8`, and `sqlfluff` to lint Python and Snowflake SQL on every Pull Request.
   * **Custom Generic Tests:** Beyond standard YAML tests, utilizes Jinja macros to enforce strict business logic (e.g., failing the pipeline if ARR is negative).
   * **Observability:** Injects automated Audit Columns (`dbt_run_id`, `dbt_updated_at`) via DRY macros for absolute row-level traceability.
4. **AI Reverse ETL (Python/LLM):**
   * Queries active prospects from the `ANALYTICS.dim_accounts` SSOT.
   * Leverages precise context engineering and parameter tuning (Temperature: 0.3) to force the LLM to output strictly formatted JSON containing dynamic lead scores and personalized sales pitches.
   * Pushes the enriched data back into a dedicated `REVERSE_ETL_OUTBOUND` schema to strictly separate operational data from analytics data.

## 1. Prerequisites

- Docker Desktop running
- A valid `.env` file in repo root
- Snowflake user/role with permissions to:
  - create/insert/truncate in `RAW` and `REVERSE_ETL_OUTBOUND` schemas
  - create/select in `ANALYTICS` schemas
- Ollama running locally with the `llama3.1` model pulled (`ollama pull llama3.1`)

Recommended `.env` keys:

```env
SNOWFLAKE_ACCOUNT=your_account_locator
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=GTM_PROJECT_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN
```

# host.docker.internal is required for reaching Ollama from inside Docker on Windows/Mac.
OLLAMA_HOST=[http://host.docker.internal:11434](http://host.docker.internal:11434)
OLLAMA_MODEL=llama3.1


## 2. Build and Start

From repo root:

```bash
docker compose build
docker compose up -d
```

Check container:

```bash
docker ps
```

Expected container name: `gtm_project`.

Bring down container and restart.
```bash
docker compose down
```

When making code changes:

```bash
docker compose up -d --build
docker exec -it gtm_project python extract_load/generate_mock_data.py
docker exec -it gtm_project bash -c "cd dbt_transform && dbt run"
docker exec -it gtm_project python reverse_etl/ai_lead_scoring.py
```

## 3. Bring Down Project

Stop services:

```bash
docker compose down
```

Stop and remove services + volumes:

```bash
docker compose down -v
```

Rebuild from scratch:

```bash
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

## 4. Troubleshooting

- `Object ... does not exist or not authorized`
  - verify dbt ran successfully,
  - verify role permissions (`USAGE`/`SELECT`/`CREATE`),
  - verify schema/table names match script expectations.

- `localhost:11434` fails inside container
  - use `OLLAMA_HOST=http://host.docker.internal:11434`.

- Intermittent JSON parse errors from Ollama
  - tighten prompt for strict JSON,
  - lower temperature,
  - add retry/validation in scoring script.

- dbt model column mismatch (e.g. `invalid identifier`)
  - align staging model column names with raw source table columns.

## 5. Project Structure

```text
├── .github/workflows/         # CI/CD pipelines (Python & SQL linting)
├── dbt_transform/             # dbt project directory
│   ├── macros/                # Custom Jinja logic (audit columns, top-n dedup, custom tests)
│   ├── models/
│   │   ├── staging/           # Defensive SQL views cleaning raw data
│   │   └── marts/             # Business-logic heavy dimensional models
├── extract_load/              # Mock data generation and raw loading
├── reverse_etl/               # AI scoring and reverse ETL writes
├── Dockerfile                 # Container definition
├── docker-compose.yml         # Service orchestration
└── requirements.txt           # Python dependencies
