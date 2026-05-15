import ast
import os
import json
import hashlib
import sys 

from datetime import datetime, timedelta
from typing import Any

import requests
import pandas as pd

sys.path.append('/opt/airflow/include/code/analytics')
from models import query_for_loop, upsert_data, initialize_database

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
    

# def basic_info_process(df):
    
    # df['file_size'] = df['file_size'].apply(lambda x: int(x))

    # size = df.file_size.sum()

    # if size != 0:
    #     if size >= 1000000000:
    #         total_size = df.file_size.sum().item()/1000000000
    #     elif size >= 1000000:
    #         total_size = df.file_size.sum().item()/1000000
    #     else:
    #         total_size = df.file_size.sum()
    
    # basic_info = pd.DataFrame([['uuid', len(df), total_size]], columns=['總檔案數', '容量總使用'])

    # return basic_info


def serach_detail_process(search_detail, logs):
    query_valueCount = search_detail.apply(lambda x : json.loads(x)['search_query'][0]).value_counts().to_frame().reset_index(names=['關鍵字']).rename(columns={'count':'數量'})
    # Filter out the search queries that are purely numeric and count the top 10 searched keywords
    query_valueCount = query_valueCount[query_valueCount['關鍵字'].isin([str(i) for i in range(10000)]) == False].head(10)

    # For the search queries with results, count how many are found and how many are failed
    query_result = search_detail.apply(lambda x : 'found' if json.loads(x)['query_id'] != "" else 'failed')#.value_counts().to_frame()
    found_or_not = (query_result.value_counts()).reset_index(name='數量').rename(columns={'details':'尋找結果計數'})

    # For the failed search queries, extract the search query keyword
    notfound = logs.loc[query_result[query_result == 'failed'].index]['details'].apply(lambda x : json.loads(x)['search_query'][0] if 'search_query' in x else 0).to_frame().rename(columns={'details':'搜尋關鍵字'})

    return query_valueCount, found_or_not, notfound

def hash_password(*args: Any) -> str:
    key = "_".join("" if arg is None else str(arg) for arg in args)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def user_behavior(df, logs , db):

    try:
        # For the search queries, extract the search query keyword and count the top 10 searched keywords
        search_detail_norm = logs[(logs.action == 'full_search')&(logs.details.str.contains('search_query'))]['details']
        search_detail_ai = logs[logs.action == 'full_search_ai']['details']

        search_detail_norm_result = serach_detail_process(search_detail_norm, logs)
        search_detail_ai_result = serach_detail_process(search_detail_ai, logs)

        upsert_data(
            'log_valuecount_query',
            search_detail_norm_result[0],
            db
        )
        upsert_data(
            'log_found_or_not',
            search_detail_norm_result[1],
            db
        )
        upsert_data(
            'log_notfound',
            search_detail_norm_result[2],
            db
        )

        data_type = (df.file_type.value_counts()).to_frame().reset_index(names='檔案類型').rename(columns={'count':'數量'})

        othe_info = df.groupby('file_type').agg({'file_size':'sum'}).reset_index(names='檔案類型').rename(columns={'file_size':'容量(KB)'})
        data_type = data_type.merge(othe_info, on='檔案類型')
        data_type['容量(KB)'] = data_type['容量(KB)']// (1024)

        upsert_data(
            'file_data_type_count',
            data_type,
            db
        )

        df_file = df.copy(deep=True)

        df_file['created_at'] = df_file['created_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) else None)
        df_file = df_file[['id', 'file_type', 'file_size',  'title', 'created_at']].rename(columns={'id':'file_id','file_type':'檔案類型', 'file_size':'檔案大小', 'title':'檔案名稱', 'created_at':'建立時間'})
        df_file['數量'] = 1

        upsert_data(
                'log_file_info',
                df_file,
                db
        )

        df_popu_keyword = search_detail_ai.apply(lambda x : json.loads(x)['search_query']).value_counts().to_frame().reset_index(names=['關鍵字']).rename(columns={'count':'數量'})

        upsert_data(
                'log_valuecount_query',
                df_popu_keyword,
                db
        )


        df_file_viewed = pd.DataFrame()

        df_preview = logs[(logs.action == 'preview_file')]

        df_file_viewed['action'] = df_preview['action']

        df_original = df_preview[['id', 'user_id', 'action', 'created_at']].reset_index(drop=True)
        df_normalized = pd.json_normalize(df_preview['details'].apply(json.loads)).reset_index(drop=True)

        df_file_viewed = pd.merge(df_original, df_normalized, left_index=True, right_index=True)
        df_file_viewed = df_file_viewed.rename(columns={'id':'log_id', 'user_id':'使用者ID', 'file_type':'檔案類型', 'file_name':'檔案名稱', 'created_at':'建立時間','action':'使用者行為', 'bucket':'資料來源'})

        df_file_viewed['使用者ID'] = df_file_viewed['使用者ID'].astype(str)

        df_file_viewed['資料來源'] = df_file_viewed['資料來源'].apply(lambda x : '本地' if x == 'raw' else x )

        logger.debug(f'df === 2\n {df_file_viewed}')

        upsert_data(
                'log_user_action',
                df_file_viewed,
                db
        )

        df_ai_search = logs[(logs.action == 'full_search_ai')]

        df_logs = df_ai_search[['user_id', 'action', 'created_at']].reset_index(drop=True)
        df_logs = df_logs.merge(pd.json_normalize(df_ai_search['details'].apply(json.loads)), left_index=True, right_index=True)
        df_logs = df_logs.merge(pd.json_normalize(df_logs['ml_response'].apply(json.loads)), left_index=True, right_index=True)

        df_logs = df_logs[['user_id', 'title','search_query']].groupby(['title','user_id']).count().reset_index().rename(columns={'user_id':'使用者ID', 'title':'標題', 'search_query':'搜尋結果數量'})

        df_logs['hash_id'] = df_logs.apply(lambda x: hash_password([x['使用者ID'], x['標題']]), axis=1)

        upsert_data(
                'log_valuecount_result',
                df_logs,
                db
        )

        response = requests.get("https://10.11.60.43:3002/public/storage_info", verify=False)

        data = response.json()

        df = pd.DataFrame([data])
        df = df[['used_disk_gb','free_disk_gb']]
        df = df.transpose().reset_index().rename(columns={'index':'儲存資訊', 0 : '數值'})
        df.loc[0,'儲存資訊'] = '已使用'
        df.loc[1,'儲存資訊'] = '未使用'

        upsert_data(
                'storage_info',
                df,
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
