import pandas as pd
from sqlalchemy import Column, Integer, String, MetaData, Table, Date, Text, text, select, BIGINT
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session

# Initialize logger
from nexva_ingestion.logger import init_log
logger = init_log('models')

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
    Column('容量', BIGINT, nullable=False)
)

Table(
    'log_query_valueCount', metadata,
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
        logger.warning(f"The DataFrame is empty from processing. Skipping upsert for {table_name}.")
        return

    # Fetch the table from metadata
    table = metadata.tables.get(table_name)
    logger.debug(f"Table columns: {[col.name for col in table.columns]}")

    if table is None:
        raise ValueError(f"Table {table_name} is not defined in metadata.")
    
    with Session(engine) as session:
        try:
            for _, row in dataframe.iterrows():
                insert_stmt = insert(table).values(**row.to_dict())
                update_dict = {col.name: row[col.name] for col in table.columns if col.name not in table.primary_key}
                on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(**update_dict)
                session.execute(on_duplicate_key_stmt)
            session.commit()
            logger.info(f"Upserted {len(dataframe)} rows into {table_name}.")
            # dataframe.to_csv(f'./csv/output_{table_name}_df.csv')
        except Exception as e:
            logger.error(f"Error during upsert in {table_name}, reason: {e}", exc_info=True)
            session.rollback()
