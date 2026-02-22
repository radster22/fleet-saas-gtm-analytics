import os
import json
from datetime import datetime

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

OLLAMA_HOST = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )


def score_prospect_with_ollama(prompt_context: str) -> dict:
    response = requests.post(
        f"{OLLAMA_HOST}/api/chat",
        json={
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": prompt_context}],
            "format": "json",
            "options": {"temperature": 0.3},
            "stream": False,
        },
        timeout=90,
    )
    response.raise_for_status()
    llm_payload = response.json()
    return json.loads(llm_payload["message"]["content"])


def analyze_and_score_leads():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    print("Fetching active prospects from dbt dim_accounts...")

    # 1. Extract: Pull behavioral and pipeline data from your dbt SSOT
    cursor.execute(
        """
        SELECT
            contact_id,
            restaurant_name as company_name,
            total_web_visits,
            current_pipeline_stage
        FROM ANALYTICS.dim_accounts
        WHERE current_pipeline_stage IN ('Prospecting', 'Qualification')
        LIMIT 10
    """
    )
    prospects = cursor.fetchall()

    # 2. Setup the Reverse ETL target table in Snowflake
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS RAW.sfdc_account_enrichment (
            contact_id VARCHAR,
            ai_lead_score INT,
            ai_efficiency_pitch VARCHAR,
            scored_at TIMESTAMP
        )
    """
    )

    enriched_data = []

    print("Running Context Engineering via LLM for Lead Scoring...")

    # 3. Transform (AI Context Engineering): Feed behavioral data to local Ollama
    for prospect in prospects:
        contact_id, company_name, web_visits, stage = prospect

        prompt_context = f"""
        You are an expert B2B SaaS Sales Analyst for a sustainable transportation platform.
        Analyze the following prospect:
        - Company: {company_name}
        - Behavioral Data: {web_visits} recent website visits.
        - Pipeline Stage: {stage}

        Task:
        1. Assign a Lead Score (1-100) based on their engagement. Higher web visits = higher score.
        2. Write a 1-sentence sales hook for the rep, focusing on bridging operational efficiency gaps and sustainable commuting.

        Output strictly as JSON: {{"score": 85, "pitch": "Your sentence here."}}
        """

        try:
            ai_result = score_prospect_with_ollama(prompt_context)

            enriched_data.append(
                (
                    contact_id,
                    ai_result["score"],
                    ai_result["pitch"],
                    datetime.now(),
                )
            )
            print(f"Scored {company_name}: {ai_result['score']}")

        except Exception as e:
            print(f"Failed to score {company_name}: {e}")

    # 4. Load (Reverse ETL): Push the AI insights back into Snowflake
    print("Pushing AI Enrichment data back to Snowflake (Simulating Reverse ETL)...")
    cursor.executemany(
        """
        INSERT INTO RAW.sfdc_account_enrichment
        (contact_id, ai_lead_score, ai_efficiency_pitch, scored_at)
        VALUES (%s, %s, %s, %s)
    """,
        enriched_data,
    )

    conn.commit()
    cursor.close()
    conn.close()
    print("Reverse ETL AI pipeline complete!")


if __name__ == "__main__":
    analyze_and_score_leads()
