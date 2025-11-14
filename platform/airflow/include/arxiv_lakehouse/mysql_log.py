import mysql.connector
from datetime import datetime
from config import mysql_conf

def store_metadata(run_id, stage_name, record_count, duration, status, component=None, note=None):
    try:
        conn = mysql.connector.connect(**mysql_conf)
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                run_id VARCHAR(50),
                stage_name VARCHAR(50),
                component VARCHAR(50),
                processed_records INT,
                duration_sec FLOAT,
                status VARCHAR(20),
                note TEXT,
                created_at DATETIME
            )
        """)

        cursor.execute("""
            INSERT INTO pipeline_metadata
            (run_id, stage_name, component, processed_records, duration_sec, status, note, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (run_id, stage_name, component, record_count, duration, status, note, datetime.now()))

        conn.commit()
    except mysql.connector.Error as err:
        print(f"❌ MySQL logging failed: {err}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        try:
            cursor.close()
            conn.close()
        except Exception:
            pass
