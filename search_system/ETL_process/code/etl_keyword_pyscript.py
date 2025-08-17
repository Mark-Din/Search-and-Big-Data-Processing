import ast
import re
import pandas as pd
import datetime
from etl_log import CreateLog
from typing import List
import nltk
nltk.download('punkt')
nltk.download('stopwords')
from collections import Counter
from nltk.corpus import stopwords
from connection import ElasticSearchConnectionManager
from common.logger import initlog

logger = initlog(__name__)


def query(time_interval):
    # Build connection
    conn = ElasticSearchConnectionManager.mysql_connection_nexva()
    peteco_nexva = conn.cursor()

    # Check if the date time is the same in Python and SQL
    date_now_python = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_now_sql_query = "select now()"
    peteco_nexva.execute(date_now_sql_query)
    date_now_sql = peteco_nexva.fetchone()
    date_now_sql = date_now_sql[0].strftime("%Y-%m-%d %H:%M:%S")

    logger.info("Date time in sql: %s", date_now_sql)

    # Set the timezone to Asia/Taipei if the date time is not the same
    if date_now_python != date_now_sql:
        logger.info("Date time is not the same: %s and %s", date_now_python, date_now_sql)
        peteco_nexva.execute("SET time_zone = 'Asia/Taipei';")

    # source_fetch_search, fetch data from search table, only fetch data from the last 24 hours
    peteco_nexva.execute(f"""
                        select
                            id,
                            uid,
                            searchKey,
                            createdAt 
                        from 
                            search
                        where
                            createdAt BETWEEN DATE_SUB(NOW(), {time_interval}) AND NOW()
                        """)
    
    # fetch data and convert to dataframe
    result_peteco_data = peteco_nexva.fetchall()
    df_result_peteco = pd.DataFrame(result_peteco_data, columns=peteco_nexva.column_names)
    logger.debug('====================df_result_peteco=======================:\n%s', df_result_peteco)
    
    # source_fetch_log, fetch data from log table, only fetch data from the last 24 hours
    peteco_nexva.execute(f'''
                        SELECT
                            uid,
                            page,
                            description,
                            createdAt
                        FROM
                            `log`
                        where
		                    page like '%addSearchDestinationLog%' and
                            createdAt BETWEEN DATE_SUB(NOW(), {time_interval}) and now()
                        order by 
                            createdAt desc
                        ''')
    
    # fetch data and convert to dataframe
    columns_search_view_action = peteco_nexva.column_names
    result_view_action_peteco_data = peteco_nexva.fetchall()
    result_view_action_peteco_data_df = pd.DataFrame(result_view_action_peteco_data, columns=columns_search_view_action)
    logger.debug('====================df_result_peteco=======================:\n%s', df_result_peteco)
    
    # Close connection
    conn.close()
    return df_result_peteco, result_view_action_peteco_data_df


def get_data_from_sql(time_interval):
    # Query data from MySQL
    df_result_peteco, result_view_action_peteco_data_df = query(time_interval)

    '''------------------------------------df_result_peteco_processing-------------------------------------'''
    # Filter out data that is not needed
    df_result_peteco = df_result_peteco[df_result_peteco.createdAt > '2022-01-01']

    # Extract date from original date
    df_result_peteco['create_date'] = df_result_peteco.createdAt.apply(lambda x: x.date())
    df_result_peteco['create_time'] = df_result_peteco.createdAt.apply(lambda x: x.time())

    # Use groupby to extract each what words were searched per person and per day
    df_SearchWord = df_result_peteco.groupby(['create_date', 'uid', 'searchKey']).agg({'createdAt': 'first'})
    df_SearchWord['count'] = 1
    df_SearchWord.reset_index(inplace=True)

    # Remove unwanted characters using regular expression substitution
    df_SearchWord['searchKey'] = df_SearchWord.searchKey.apply(lambda x: re.sub(r'[^\w\s]+', '', x))

    # Filter out empty strings
    df_SearchWord = df_SearchWord.iloc[[index for index in range(len(df_SearchWord)) if df_SearchWord['searchKey'][index]], :]
    df_SearchWord.reset_index(drop=True, inplace=True)


    '''--------------------------result_view_action_peteco_data_df_processing------------------------------'''
    # Extract date from original date
    result_view_action_peteco_data_df = result_view_action_peteco_data_df[result_view_action_peteco_data_df.createdAt > '2022-01-01']
    result_view_action_peteco_data_df.drop(index=result_view_action_peteco_data_df[result_view_action_peteco_data_df.page == '/nexva/myChannel'].index, inplace=True)
    result_view_action_peteco_data_df.reset_index(drop=True, inplace=True)
    result_view_action_peteco_data_df.sort_values('createdAt', ascending=False, inplace=True)

    return df_SearchWord, result_view_action_peteco_data_df


def word_processing(df_SearchWord: pd.DataFrame):
    # Define your custom stop words
    english_alphabet = ['test', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    chinese_alphabet = ['測試', 'ㄅ', 'ㄆ', 'ㄇ', 'ㄈ', 'ㄉ', 'ㄊ', 'ㄋ', 'ㄌ', 'ㄍ', 'ㄎ', 'ㄏ', 'ㄐ', 'ㄑ', 'ㄒ', 'ㄓ', 'ㄔ', 'ㄕ', 'ㄖ', 'ㄗ', 'ㄘ', 'ㄙ', 'ㄚ', 'ㄛ', 'ㄜ', 'ㄝ', 'ㄞ', 'ㄟ', 'ㄠ', 'ㄡ', 'ㄢ', 'ㄣ', 'ㄤ', 'ㄥ', 'ㄦ', 'ㄧ', 'ㄨ', 'ㄩ']

    # Add your custom stop words to the NLTK English stop words
    english_stop_words = set(stopwords.words("english") + english_alphabet)
    chinese_stop_words = set(stopwords.words("chinese") + chinese_alphabet)

    # Remove stopwords
    searchKey = df_SearchWord['searchKey']
    drop_index = []
    for index in range(len(searchKey)):
        if (len(set(searchKey[index])) == 1) and (list(set(searchKey[index]))[0].strip().lower() in english_stop_words): 
            drop_index.append(index)
        elif searchKey[index] in chinese_stop_words:
            drop_index.append(index)

    df_SearchWord = df_SearchWord.drop(drop_index)
    df_SearchWord.reset_index(drop=True, inplace=True)
    df_SearchWord['searchKey'] = df_SearchWord.searchKey.apply(lambda x: x.strip().lower())

    # Adjust same meaning 
    df_SearchWord['searchKey'] = df_SearchWord['searchKey'].apply(lambda x: '狗' if x == '狗狗' else '貓' if x == '貓咪' else x)
    df_SearchWord.sort_values('create_date', ascending=False, inplace=True)

    # Display the most frequent words and their importance  
    return df_SearchWord


def action_processing(result_view_action_peteco_data_df: pd.DataFrame):

    result_view_action_peteco_data_df['description'] = result_view_action_peteco_data_df.description.apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else None)
    result_view_action_peteco_data_df['uid'] = result_view_action_peteco_data_df.description.apply(lambda x: x['uid'])
    result_view_action_peteco_data_df['page'] = result_view_action_peteco_data_df.description.apply(lambda x: x['selectedType'])
    
    # Sort by columns and time.
    result_view_action_peteco_data_df = result_view_action_peteco_data_df[['uid', 'page', 'createdAt']]
    result_view_action_peteco_data_df = result_view_action_peteco_data_df.sort_values(by='createdAt', ascending=False)
    
    return result_view_action_peteco_data_df


def insert_data_to_mysql_adjusted(table_name, df: pd.DataFrame, conn=None):
    if conn is None:
        conn = ElasticSearchConnectionManager.mysql_connection_nexva()
        cursor = conn.cursor()

    try:
        for row in df.intertuples(index=False):
            cursor.execute(
                "Insert into keyword_processed_search (create_date, uid, searchKey, createdAt, count) VALUES (%s, %s, %s, %s, %s)",
                (row.create_date, row.uid, row.searchKey, row.createdAt, row.count) 
            )
        return True
    except Exception as e:
        logger.error('Error in inserting data in mysql {e}', exc_info=True)
        return False

def etl_search():
    # Set time interval, default is 60 minutes.
    time_interval = 'interval 12 hour'
    # Get data from MySQL
    word_df, view_action_df = get_data_from_sql(time_interval)

    logger.debug('====================word_df=======================:\n%s', word_df)
    logger.debug('====================view_action_df=======================:\n%s', view_action_df)
    # Processing Keyword Search
    search_df = word_processing(word_df)
    logger.debug('====================search_df=======================:\n%s', search_df)

    # Processing View Action
    action_df = action_processing(view_action_df)

    conn = ElasticSearchConnectionManager.mysql_connection_nexva()

    # Load data from MySQL, and remove the data that has been loaded
    loaded_data = pd.read_sql(f'SELECT * FROM keyword_processed_action where createdAt > date_sub(now(), {time_interval})', con=conn)
    logger.debug('====================loaded_data=======================:\n%s', loaded_data)

    action_df = action_df[~action_df.createdAt.isin(loaded_data.createdAt)]
    loaded_data = pd.read_sql(f'SELECT * FROM keyword_processed_search where createdAt > date_sub(now(), {time_interval})', con=conn)
    search_df = search_df[~search_df.createdAt.isin(loaded_data.createdAt)]
    logger.debug('====================search_df=======================:\n%s', search_df)

    if search_df.empty and action_df.empty:
        logger.info('No new data to process.')
        CreateLog(collection_name='keyword_processed_search', update_data_count=0, create_data_count=0).success()
        CreateLog(collection_name='keyword_processed_action', update_data_count=0, create_data_count=0).success()
        return
    
    # Insert data to MySQL, and create log
    result = insert_data_to_mysql_adjusted('keyword_processed_search', search_df)
    if result:        
        CreateLog(collection_name='keyword_processed_search', update_data_count=len(search_df), create_data_count = len(search_df)).success()
    else:
        CreateLog(collection_name='keyword_processed_search', update_data_count=len(search_df), create_data_count = len(search_df)).fail()

    result = insert_data_to_mysql_adjusted('keyword_processed_action', action_df)
    if result:        
        CreateLog(collection_name='keyword_processed_action', update_data_count=len(action_df), create_data_count=len(action_df)).success()
    else:
        CreateLog(collection_name='keyword_processed_action', update_data_count=len(action_df), create_data_count=len(action_df)).fail()


if __name__ == "__main__":
    etl_search()
