import ast
import os
import json

from datetime import datetime, timedelta
from .models import query_for_loop, upsert_data, initialize_database

from common.connection import DatabaseConnection, create_database
from common.utils import batch_convert_uuid, batch_shift_date, asia_time_zone
from common.config import config_43_mysql, config_pg
from common.init_log import initlog

import warnings
warnings.filterwarnings("ignore")

config_msyql = config_43_mysql

logger = initlog("analytics_main")

def get_db_connection(db_type, config, db_name=None):

    if db_type == 'mysql':
        return DatabaseConnection.mysql_connection(config, db_name)
    elif db_type == 'postgres':
        return DatabaseConnection.postgres_connection(config, db_name)
    else:
        raise ValueError(f"Invalid database type: {db_type}")
    

def user_behavior(df, logs , db):

    try:
        # For the search queries, extract the search query keyword and count the top 10 searched keywords
        query_valueCount = logs[logs.action == 'full_search']['details'].apply(lambda x : json.loads(x)['search_query'][0]).value_counts().to_frame().reset_index(names=['關鍵字']).rename(columns={'count':'數量'})
        query_valueCount = query_valueCount[query_valueCount['關鍵字'].isin([str(i) for i in range(10)]) == False].head(10)

        # For the search queries with results, count how many are found and how many are failed
        query_result = logs[(logs.action == 'full_search')&(logs.details.str.contains('result'))]['details'].apply(lambda x : 'found' if json.loads(x)['result'] != [] else 'failed')#.value_counts().to_frame()
        found_or_not = (query_result.value_counts()).reset_index(name='數量').rename(columns={'details':'尋找結果計數'})

        # For the failed search queries, extract the search query keyword
        notfound = logs.loc[query_result[query_result == 'failed'].index]['details'].apply(lambda x : json.loads(x)['search_query'][0] if 'search_query' in x else 0).to_frame().rename(columns={'details':'搜尋關鍵字'})

        upsert_data(
            'log_query_valueCount',
            query_valueCount,
            db
        )
        upsert_data(
            'log_found_or_not',
            found_or_not,
            db
        )
        upsert_data(
            'log_notfound',
            notfound,
            db
        )

        data_type = (df.file_type.value_counts()).to_frame().reset_index(names='檔案類型').rename(columns={'count':'數量'})

        othe_info = df.groupby('file_type').agg({'file_size':'sum'}).reset_index(names='檔案類型').rename(columns={'file_size':'容量'})
        data_type = data_type.merge(othe_info, on='檔案類型')

        df['file_size'] = df['file_size'].apply(lambda x: int(x))

        # size = df.file_size.sum()

        # if size != 0:
        #     if size >= 1000000000:
        #         total_size = df.file_size.sum().item()/1000000000
        #     elif size >= 1000000:
        #         total_size = df.file_size.sum().item()/1000000
        #     else:
        #         total_size = df.file_size.sum()
        
        # basic_info = pd.DataFrame([['uuid', len(df), total_size]], columns=['總檔案數', '容量總使用'])

        upsert_data(
            'file_data_type_count',
            data_type,
            db
        )
    except Exception as e:
        logger.error(f'Error processing data {e}', exc_info=True)
        raise 


def process_etl_data(
            df_files, df_logs, engine_with_mysql_db
        ):
    """
    Executes the ETL process by validating and inserting data into MySQL.
    """

    logger.info("{-----UPLOADING DATA TO MYSQL DATABASE-----}")

    # Define processing steps and corresponding DataFrames
    process_steps = [
        ("df_files", df_files, 
            [
            lambda df: user_behavior(df, df_logs, engine_with_mysql_db)
            ]
        ),
    ]

    # Process each step
    for step_name, dfs, functions in process_steps:
        if isinstance(dfs, list):
            # For multi-DataFrame processes
            if any(df.empty for df in dfs):
                logger.info(f"Skipping '{step_name}' - One or more DataFrames are empty.")
                continue
        else:
            # For single DataFrame processes
            if dfs.empty:
                logger.info(f"Skipping '{step_name}' - DataFrame is empty.")
                continue

        # Apply processing functions
        for func in functions:
            func(dfs)  # Pass DataFrame(s) to function

    logger.info("ETL process completed successfully.")


# def preprocess_data(df):

#     if df.empty:
#         logger.info("No rows to process in preprocess_data.")
#         return df
    
#     df['點選種類'] = df['status']

#     def update_columns(row):
#         # Step 1: Update the status column
#         if type(row['metadata']) == str:
#             row['metadata'] = ast.literal_eval(row['metadata'])

#         if row['status'] in ['開啟 qrcode', '點擊文字連結', '點擊圖片連結']:
#             if row['metadata'].get('target') is not None:
#                 if 'questionnaire_option' in row['metadata']['target']:
#                     row['status'] = '點擊問卷'
#                 else:
#                     row['status'] = '點擊連結'
        
#         # Step 2: Update 點選種類 column based on the updated status
#         if row['點選種類'] == '點擊文字連結' and row['status'] == '點擊問卷':
#             row['點選種類'] = '點擊答案'
        
#         return row

#     # Apply the function to the entire DataFrame
#     df = df.apply(update_columns, axis=1)
#     df['str_metadata'] = df['metadata'].apply(str)
#     df.drop_duplicates(subset=['edm_id', 'email', '點選種類', 'str_metadata'], keep='first', inplace=True)  
#     df.drop(columns=['str_metadata'], inplace=True)

#     df = df[(df['create_date'] != '0')&(df['create_date'] != 0)&(df['create_date'].notnull())&(df['edm_subject'] != '登入通知信')&(df['edm_subject'] != '衝撞系')]

#     return df


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))

    try:
        logger.info(f"=================Start ETL Process================")
        # Loadding the latest update time
        file = 'last_run_time.json'
        file_path = os.path.join(script_dir, file)
        try:
            with open(file_path,'r') as f: 
                last_run_time = json.load(f)
                last_run_time_filter = last_run_time['last_run_time']
                last_run_time_display = datetime.fromisoformat(last_run_time_filter)
            logger.info(f"last_run_time: [{(last_run_time_display + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')}]]")
        except FileNotFoundError:
            # Set the initial date for the first run
            last_run_time_filter = '2025-01-01'
            logger.info(f"last_run_time: {last_run_time_filter}]")
        
        # First, connect to MySQL without specifying a database
        engine_without_db = get_db_connection('mysql', config_msyql)

        # Create the database
        create_database(engine_without_db, 'qidu_quickreport')
    
        # Now, connect to the newly created database
        engine_with_mysql_db = get_db_connection('mysql', config_msyql, 'qidu_quickreport')
        # connect to the postgreSQL database
        engine_with_pg_files = get_db_connection('postgres', config_pg, 'storage_search')

        # Initialize the tables in the MySQL database
        initialize_database(engine_with_mysql_db)

        # # Delete the config.json file if it exists
        # file = os.path.join(script_dir, 'config.json')
        # if os.path.exists(file):
        #     os.remove(file)
        #     logger.info("config.json file removed.")
        # else:
        #     logger.info("config.json file does not exist. Try other path")
        #     file = os.path.join(script_dir, 'config_concord.json')
        #     if os.path.exists(file):
        #         os.remove(file)
        #         logger.info("config.json file removed.")
        #     else:
        #         logger.info("config.json file does not exist. No removal action happened")

        # Query the tables and return the results as DataFrames
        with engine_with_pg_files.connect() as connection:
           df_files, df_logs=  query_for_loop('files', 'logs', connection=connection, last_run_time=last_run_time_filter, database_name='storage_search')

        # Save the time for next run
        file = 'last_run_time.json'
        file_path = os.path.join(script_dir, file)
        with open(file_path,'w') as f:
            json.dump({'last_run_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, f)
        
        # Close the connections
        engine_with_pg_files.dispose()

        # Convert UUID columns for all DataFrames in one line
        df_files, df_logs = batch_convert_uuid(
            [df_files, df_logs]
        )

        # Convert UUID columns for all DataFrames in one line
        df_files, df_logs = batch_shift_date(
            [df_files, df_logs]
        )

        logger.info("{-----LENGTH OF DATAFRAMES after date shifted and uuid converted.-----}")

        # Get the length of each DataFrame
        table_names = ['df_files', 'df_logs']
        df_for_each = [df_files, df_logs]

        for df_,name in zip(df_for_each, table_names):  
            logger.info(f"[{len(df_)}] number of rows in {name} DataFrame.")
            # df_.to_csv(f"./csv/{name}.csv", index=False)

        # Preprocess the data
        # df_files = preprocess_data(df_files)

        # Process the ETL data
        process_etl_data(
            df_files, df_logs, engine_with_mysql_db
        )

        engine_with_mysql_db.dispose()

        # Format the time string
        successfull_time = asia_time_zone().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"Data saved successfully. saved time: [{successfull_time}]")
    
    except Exception as e:
        logger.error(f"Error in main ETL process: {e}", exc_info=True)
        raise e
    finally:
        logger.info(f"===================End ETL Process==================")


if __name__ == "__main__":
    
    main()
