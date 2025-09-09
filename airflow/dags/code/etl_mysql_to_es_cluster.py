from elasticsearch import helpers
import numpy as np
import pandas as pd
import datetime, boto3
import es_mapping
from connection import ElasticSearchConnectionManager
import sys

sys.path.append(r'C:\Users\mark.ding\big-data-ai-integration-platform\common')
from logger import initlog

logger = initlog(__name__)

# JSON serializer for datetime and bytearray objects
def json_serial(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    if isinstance(obj, bytearray):
        return obj.decode('utf-8')
    raise TypeError(f"Type {type(obj)} not serializable")

# Fetch data from MySQL in batches
def fetch_data(cursor, batch_size=1000):
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        cleaned_rows = [
            {k: json_serial(v) if isinstance(v, (datetime.datetime, bytearray)) else v for k, v in row.items()}
            for row in rows
        ]
        logger.debug(f"Fetched {len(cleaned_rows)} rows from MySQL.")
        yield cleaned_rows

# Search and update document in Elasticsearch
def search_and_update_document(es, index, source_id, update_body):
    try:
        search_query = {"query": {"term": {"id": source_id}}}
        search_response = es.search(index=index, body=search_query)
        hits = search_response['hits']['hits']
        
        if not hits:
            logger.info(f"Document with source ID {source_id} does not exist.")
            return False

        for hit in hits:
            doc_id = hit['_id']
            response = es.update(index=index, id=doc_id, body={"doc": update_body})
            # logger.info(f"Document with source ID {source_id} updated: {response}")
            return True

    except Exception as e:
        logger.error(f"Error updating document with source ID {source_id}: {e}")
        return False

# Bulk import data to Elasticsearch
def elastic_import(es, actions):
    try:
        response, errors = helpers.bulk(es, actions, raise_on_error=False)
        logger.info(f"Imported {response} documents to Elasticsearch, errors: {errors}")
    except Exception as e:
        logger.error(f"Bulk import errors: {e}")

# Update data in Elasticsearch
def update_data_to_es(es, cursor, es_index, table_name):
    updated_count = 0
    create_data_count = 0
    actions = []

    for batch in fetch_data(cursor):
        logger.debug(f"Processing batch with {len(batch)} records.")
        
        df = pd.DataFrame(batch)
        df = df.where(pd.notnull(df), None)  # Replace NaN with None
        df = df.replace({np.nan: None})  # Replace NaN with None

        batch = df.to_dict(orient='records')

        for row in batch:
            source_id = row['統一編號']
            # logger.info(f"Updating document with source ID {source_id}...")
            try:
                doc_exists = search_and_update_document(es, es_index, source_id, row)
                if not doc_exists:
                    actions.append({"_index": es_index, "_source": row})
                    create_data_count += 1
                    logger.debug(f"Document with source ID {source_id} created.")
                else:
                    updated_count += 1
                    logger.debug(f"Document with source ID {source_id} updated.")

            except Exception as e:
                logger.error(f"Error updating document with source ID {source_id}: {e}")

        if actions:
            logger.info(f"Importing {len(actions)} documents to Elasticsearch index: [{es_index}]...]")
            elastic_import(es, actions)
            actions = []  # Clear actions after bulk import

    logger.info(f"[{table_name}] total documents updated: {updated_count}, and created: {create_data_count}")
    return updated_count, create_data_count

# Main function to handle the ETL process
def etl_process(table_name, es, es_index):
    logger.info(f"Starting ETL process for table: {table_name}.")
    
    # Create MySQL connection
    mysql_conn = ElasticSearchConnectionManager.mysql_connection_whole_corp() 
    cursor = mysql_conn.cursor(dictionary=True)

    date_now_python = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_now_sql_query = "SELECT now()"
    cursor.execute(date_now_sql_query)
    date_now_sql = cursor.fetchone()
    date_now_sql = date_now_sql['now()'].strftime("%Y-%m-%d %H:%M:%S")

    # Check if date time is the same in Python and SQL
    if date_now_python != date_now_sql:
        logger.info("Date time is not the same: %s and %s", date_now_python, date_now_sql)
        cursor.execute("SET time_zone = 'Asia/Taipei';")
    else:
        logger.info("Date time is the same: %s", date_now_python)

    # Query to fetch updated records
    sql_query_updatedAt = f"SELECT * FROM {table_name}"
    cursor.execute(sql_query_updatedAt)

    # Update data in Elasticsearch
    update_data_count, create_data_count = update_data_to_es(es, cursor, es_index, table_name)

    mysql_conn.close()
    logger.info(f"ETL process for table: {table_name} completed with {update_data_count} updates and {create_data_count} creations.")
    return update_data_count, create_data_count


# def ml_process(df):

#     # Get sparse vectorizer from minio
    
#     # Get kmeans and svd from minio
#     s3 = boto3.client(
#         "s3",
#         endpoint_url = 'http://minio:9000',
#         aws_access_key_id="minioadmin",
#         aws_secret_access_key="minioadmin"
#     )
#     s3.download_file(" deltabucket", "models/sparseVector", "/tmp/sk_vectorizer.pkl")

#     logger.info("Starting ML processing.")

#     df['scaled_data'] = PythonVectorizer.fit_transform(df)
    
#     # Modeling
#     data, vectorizer, nmf_model, pca, kmeans, df_encoded = vectorize_and_model(data, scaler)
    
#     final_df = course_df.copy(deep=True)
#     final_df['attributes_vector'] = df_encoded.apply(lambda row: row.tolist(), axis=1)
#     final_df = final_df.where(pd.notnull(final_df), None)

#     logger.info("ML processing completed.")
#     return final_df


def create_index_if_not_exists(es, index_name):
    try:
        es.indices.get_mapping(index=index_name)
        logger.info(f"Index {index_name} already exists.")
    except Exception as e:
        logger.warning(f"Index {index_name} does not exist. Creating new index.")
        mapping = es_mapping.corp_mapping()
        if mapping:
            es.indices.create(index=index_name, body=mapping)
            logger.info(f"Index {index_name} created with the specified mapping.")
        else:
            logger.error(f"Failed to create index {index_name}. No valid mapping found.")


def main():
    logger.info("ETL process started.")
    es = ElasticSearchConnectionManager.get_instance()
    
    table_name = 'wholecorp_clusters_vector'
    
    index_name = table_name.lower()
    
    create_index_if_not_exists(es, index_name)
    
    update_data_count, create_data_count = etl_process(table_name, es, index_name)
    
    if update_data_count > 0 or create_data_count > 0:
        CreateLog(table_name, update_data_count, create_data_count).success()
    
    logger.info(f"Data update for {table_name} completed, with update_data_count: {update_data_count}, create_data_count: {create_data_count}")
    
    logger.info("ETL process finished.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"ETL process failed due to: {str(e)}", exc_info=True)
