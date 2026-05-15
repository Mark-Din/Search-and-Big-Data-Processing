import pandas as pd
import sys
import os
from sqlalchemy import Column, DateTime, Integer, String, MetaData, Table, Date, Text, func, text, select, BIGINT, TIMESTAMP 
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

script_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(r'D:\markding_git\big-data-ai-integration-platform\platform\airflow\include\code')
# Initialize logger
from common.init_log import initlog
logger = initlog('models')

import warnings
warnings.filterwarnings("ignore")

# Initialize metadata
metadata = MetaData()


# Define all tables in metadata
Table(
    'file_data_type_count', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('檔案類型', String(100), unique=True),
    Column('數量', Integer, nullable=False),
    Column('容量(KB)', BIGINT, nullable=False)
)

Table(
    'log_valuecount_query', metadata,
    Column('id',Integer, primary_key=True, autoincrement=True),
    Column('關鍵字',String(50), unique=True),
    Column('數量', Integer)
)

Table(
    'log_found_or_not', metadata,
    Column('id',Integer, primary_key=True, autoincrement=True),
    Column('尋找結果計數', String(100), unique=True,  nullable=False),
    Column('數量', String(100), nullable=False),
)

Table(
    'log_notfound', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('搜尋關鍵字', String(100), nullable=False, unique=True),
)

Table(
    "log_file_info", metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("file_id", Integer, unique=True),

    Column("檔案類型", String(20)),
    Column("檔案大小", BIGINT),

    Column("檔案名稱", Text),
    Column("數量", Integer, nullable=False),

    Column(
        "建立時間",
        DateTime,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False
    )
)

Table(
    "log_user_action",
    metadata,

    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("log_id", Integer, unique=True),

    Column("使用者ID", String(20), nullable=False),

    Column("使用者行為", String(100), nullable=False),

    Column("檔案名稱", String(255)),
    Column("檔案類型", String(20)),
    Column("資料來源", String(100)),

    Column(
        "建立時間",
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False
    )
)

Table(
    "log_valuecount_result",
    metadata,

    Column("id", Integer, primary_key=True, autoincrement=True),

    Column("使用者ID", String(20), nullable=False),
    Column("標題", String(100), nullable=False),
    Column("搜尋結果數量", Integer, nullable=False),
    Column('hash_id', String(64), unique=True, nullable=False),

    # Column(
    #     "建立時間",
    #     DateTime(timezone=True),
    #     server_default=text("CURRENT_TIMESTAMP")
    # )
)

Table(
    "storage_info",
    metadata,

    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("儲存資訊", String(50), unique=True, nullable=False),
    Column("數值", Integer, nullable=False),

    # Column(
    #         "建立時間",
    #         TIMESTAMP(timezone=True),
    #         server_default=func.now(),
    #         nullable=False
    #     )
)

# Initialize tables at startup
def initialize_database(engine):
    metadata.create_all(engine)


# Function to query multiple tables and return DataFrames
def query_for_loop(*args, connection, last_run_time, database_name):
    tuple_df = ()
    for table in args:
        try:
            print(f'table:===={table}')
            sql_query = text(f"SELECT * FROM {table}")

            result = connection.execute(sql_query, {'last_run_time': last_run_time})

            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            logger.info(f"Queried [{len(df)}] rows from table [{table}], database [{database_name}]. query:====={sql_query}")
            tuple_df += (df,)
        except Exception as e:
            logger.error(f"Error querying table {table}: {e}")
    return tuple_df


# Function to perform an upsert operation
def upsert_data(table_name, dataframe, engine):

    logger.debug(f"DataFrame columns: {dataframe.columns}")

    if dataframe.empty:
        logger.warning(
            f"The DataFrame is empty from processing. "
            f"Skipping upsert for {table_name}."
        )
        return

    table = metadata.tables.get(table_name)

    if table is None:
        raise ValueError(f"Table {table_name} is not defined in metadata.")

    logger.debug(f"Table columns: {[col.name for col in table.columns]}")

    with Session(engine) as session:
        try:

            for _, row in dataframe.iterrows():

                row_dict = row.to_dict()

                insert_stmt = insert(table).values(**row_dict)

                update_dict = {
                    col.name: row_dict[col.name]
                    for col in table.columns
                    if not col.primary_key
                }

                on_duplicate_key_stmt = (
                    insert_stmt.on_duplicate_key_update(**update_dict)
                )

                session.execute(on_duplicate_key_stmt)

            session.commit()

            logger.info(
                f"Upserted {len(dataframe)} rows into {table_name}."
            )

        except Exception as e:
            logger.error(
                f"Error during upsert in {table_name}, reason: {e}",
                exc_info=True
            )
            session.rollback()
