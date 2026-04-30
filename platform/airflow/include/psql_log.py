import psycopg2
import sys
from datetime import datetime

sys.path.append('/opt/airflow/include/')
from config import config_pg  # rename config

def store_metadata(run_id, stage_name, record_count, duration, status, component=None, note=None):
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**config_pg)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                id SERIAL PRIMARY KEY,
                run_id VARCHAR(50),
                stage_name VARCHAR(50),
                component VARCHAR(50),
                processed_records INTEGER,
                duration_sec DOUBLE PRECISION,
                status VARCHAR(20),
                note TEXT,
                created_at TIMESTAMP
            )
        """)

        cursor.execute("""
            INSERT INTO pipeline_metadata
            (run_id, stage_name, component, processed_records, duration_sec, status, note, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id,
            stage_name,
            component,
            record_count,
            duration,
            status,
            note,
            datetime.now()
        ))

        conn.commit()

    except psycopg2.Error as err:
        print(f"❌ PostgreSQL logging failed: {err}")
        if conn:
            conn.rollback()

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()