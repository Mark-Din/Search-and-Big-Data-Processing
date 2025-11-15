import pytest
import time
from minio import Minio
import mysql.connector

@pytest.mark.system
def test_etl_end_to_end():
    time.sleep(10)  # wait for ETL

    # verify cleaned table
    db = mysql.connector.connect(
        host="mysql_db_container",
        port=3307,
        user="root",
        password="!QAZ2wsx",
        database="arxiv"
    )
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM pipeline_metadata")
    count = cursor.fetchone()[0]

    assert count > 0, "ETL pipeline produced no cleaned rows"
