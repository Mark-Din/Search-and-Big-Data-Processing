from datetime import datetime, timezone, timedelta
from mysql.connector import connect
from connection import ElasticSearchConnectionManager

class CreateLog:

    def __init__(self, collection_name, update_data_count, create_data_count):
        self.collection_name = collection_name
        self.update_data_count = update_data_count
        self.create_data_count = create_data_count
        self.conn = ElasticSearchConnectionManager.mysql_connection_nexva()
        self.query = (""" INSERT INTO ETL_log (from_which_collection, createTime, update_data_count, create_data_count, status)
                          VALUES (%s, %s, %s, %s, %s)
                      """)
        self.getTimeNow = datetime.now(tz=timezone(timedelta(hours=8)))

    def create(self, status):
        log_data = [self.collection_name,
                    self.getTimeNow, self.update_data_count, self.create_data_count, status]
        cursor = self.conn.cursor()

        # Check if the table exists, if not create it
        cursor.execute("""
                       CREATE TABLE IF NOT EXISTS ETL_log (
                           id INT AUTO_INCREMENT PRIMARY KEY,
                           from_which_collection VARCHAR(50),
                           createTime DATETIME,
                           update_data_count INT(50),
                           create_data_count INT(50),
                           status CHAR(1)
                       )
                       """)
        cursor.execute(self.query, log_data)
        self.conn.commit()
        cursor.close()
        self.conn.disconnect()

    def success(self):
        self.status = "S"
        self.create(self.status)

    def fail(self):
        self.status = "F"
        self.create(self.status)
