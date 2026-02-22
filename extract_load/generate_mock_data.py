import os
import random
import uuid
from datetime import datetime, timedelta
import snowflake.connector
from faker import Faker
from dotenv import load_dotenv

# Load credentials from the .env file
load_dotenv()

fake = Faker()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )

def generate_and_load_data():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    print("Connected to Snowflake. Creating tables...")

    # 1. Create Raw Tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_hubspot_contacts (
            contact_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            restaurant_name VARCHAR,
            contact_created_at TIMESTAMP
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_hubspot_web_visits (
            visit_id VARCHAR,
            contact_id VARCHAR,
            utm_source VARCHAR,
            page_visited VARCHAR,
            visit_timestamp TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS raw_sfdc_opportunities (
            opportunity_id VARCHAR,
            contact_id VARCHAR,
            stage VARCHAR,
            arr_amount FLOAT,
            created_date TIMESTAMP,
            close_date TIMESTAMP
        )
    """)

    # Clear old data for idempotency (so you can run this multiple times safely)
    cursor.execute("TRUNCATE TABLE raw_hubspot_contacts")
    cursor.execute("TRUNCATE TABLE raw_hubspot_web_visits")
    cursor.execute("TRUNCATE TABLE raw_sfdc_opportunities")

    print("Generating mock GTM data...")
    
    contacts = []
    web_visits = []
    opportunities = []
    
    utm_sources = ['LinkedIn B2B', 'Local Chamber of Commerce', 'Google Ads (Eco)', 'Direct', 'Partner Referral']
    pages = ['/corporate-partnerships', '/co2-impact-calculator', '/carpool-subsidy-pricing', '/book-demo']
    stages = ['Prospecting', 'Qualification', 'Pilot Program', 'Closed Won', 'Closed Lost']
    institution_types = ['University', 'Corporate Office', 'Hospital System', 'Large Restaurant Group']

    # Generate 100 Restaurant Leads
    for _ in range(100):
        contact_id = str(uuid.uuid4())
        created_at = fake.date_time_between(start_date='-1y', end_date='now')
        
        # Generate a business name and an HR/Sustainability contact
        company_name = f"{fake.city()} {random.choice(institution_types)}"
        
        contacts.append((
            contact_id,
            fake.first_name(),
            fake.last_name(),
            company_name,
            created_at
        ))

        # Generate web visits to simulate how businesses find the carpool program
        for _ in range(random.randint(1, 5)):
            visit_time = created_at + timedelta(days=random.randint(0, 30))
            web_visits.append((
                str(uuid.uuid4()),
                contact_id,
                random.choice(utm_sources),
                random.choice(pages),
                visit_time
            ))

        # Generate Salesforce Opportunities for Corporate Partnerships
        if random.random() < 0.40:
            stage = random.choice(stages)
            opp_created = created_at + timedelta(days=random.randint(5, 45))
            close_date = opp_created + timedelta(days=random.randint(10, 90)) if stage in ['Closed Won', 'Closed Lost'] else None
            # ARR represents the annual value of the commuter subsidy contract
            arr_amount = round(random.uniform(5000.0, 50000.0), 2) 

            opportunities.append((
                str(uuid.uuid4()),
                contact_id,
                stage,
                arr_amount,
                opp_created,
                close_date
            ))

    print("Inserting data into Snowflake...")
    
    # Bulk insert using executemany
    cursor.executemany("INSERT INTO raw_hubspot_contacts VALUES (%s, %s, %s, %s, %s)", contacts)
    cursor.executemany("INSERT INTO raw_hubspot_web_visits VALUES (%s, %s, %s, %s, %s)", web_visits)
    cursor.executemany("INSERT INTO raw_sfdc_opportunities VALUES (%s, %s, %s, %s, %s, %s)", opportunities)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data loaded successfully! 🚀")

if __name__ == "__main__":
    generate_and_load_data()