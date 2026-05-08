import os
from etl.code.qr_analysis.main import get_config
from database import DatabaseConnection

from decrypt import get_config
from logger import init_log

from sqlalchemy import text

logger = init_log('delete_logs')

def main():
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))

        DB_NAMES = ["edm", "segdb", "campaign", "accounts"]

        for db in DB_NAMES:
            DB_NAME = db

            try:
                config = get_config(script_dir)
                engine = DatabaseConnection.postgres_connection(config, db)

            except Exception as error:
                print(f"Error connecting to PostgreSQL: {error}")
                continue  # skip to next DB if failed

            with engine.begin() as connection:
                results = connection.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name != 'goose_db_version'
                """))

                for table in results:
                    if table[0] == 'logs':
                        delete_result = connection.execute(
                            text("DELETE FROM logs WHERE created_at < CURRENT_DATE - INTERVAL '210 day'")
                        )
                        logger.info(f"Deleted {delete_result.rowcount} rows from logs in {DB_NAME}")
                        
        logger.info('============Deleting process ends=============')

    except Exception as e:
        logger.exception('Something wrong in the deleting process')

    finally:
        if 'engine' in locals():
            engine.dispose()

if __name__ == "__main__":
    logger.info('===============Deleting logs in database process begins=============')
    main()
