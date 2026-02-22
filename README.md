# Fleet SaaS GTM Analytics

End-to-end local workflow for:
- loading mock GTM data into Snowflake,
- transforming with dbt,
- running AI lead scoring reverse ETL,
- and preparing data for BI tools.

## 1. Prerequisites

- Docker Desktop running
- A valid `.env` file in repo root
- Snowflake user/role with permissions to:
  - create/insert/truncate in raw/reverse-etl schemas
  - create/select in analytics schemas

Recommended `.env` keys:

```env
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=GTM_PROJECT_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ACCOUNTADMIN

# Optional for local Ollama
OLLAMA_HOST=http://host.docker.internal:11434
OLLAMA_MODEL=llama2
```

Notes:
- `host.docker.internal` is preferred for reaching Ollama from inside Docker.
- If Ollama runs outside Docker, keep it running before AI scoring.

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

## 3. Generate Mock Source Data

Run the extract/load script:

```bash
docker exec -it gtm_project python extract_load/generate_mock_data.py
```

What it does:
- creates `RAW` tables if missing,
- truncates old data,
- inserts fresh mock contacts, web visits, and opportunities.

## 4. Run dbt Transformations

Run dbt models:

```bash
docker exec -it gtm_project bash -c "cd dbt_transform && dbt run"
```

Optional quality checks:

```bash
docker exec -it gtm_project bash -c "cd dbt_transform && dbt test"
```

Expected key outputs:
- `ANALYTICS.dim_accounts`
- `ANALYTICS.fct_revenue_attribution`

## 5. Run AI Lead Scoring Reverse ETL

Run the scoring pipeline:

```bash
docker exec -it gtm_project python reverse_etl/ai_lead_scoring.py
```

Expected behavior:
- reads prospects from `ANALYTICS.dim_accounts`,
- creates reverse ETL schema/table if configured in script,
- writes scored rows to `sfdc_account_enrichment`.

## 6. Quick Verification (Optional)

Open a shell in the container:

```bash
docker exec -it gtm_project bash
```

Then run quick Snowflake checks with Python or your SQL client, for example:
- count rows in `ANALYTICS.dim_accounts`
- count rows in reverse ETL table

## 7. Common Restart Flow

When making code changes:

```bash
docker compose up -d --build
docker exec -it gtm_project python extract_load/generate_mock_data.py
docker exec -it gtm_project bash -c "cd dbt_transform && dbt run"
docker exec -it gtm_project python reverse_etl/ai_lead_scoring.py
```

## 8. Bring Down Project

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

## 9. Troubleshooting

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

## 10. Project Structure

```text
extract_load/      # mock data generation and raw loading
dbt_transform/     # dbt project (staging + marts)
reverse_etl/       # AI scoring and reverse ETL writes
Dockerfile
docker-compose.yml
requirements.txt
```
