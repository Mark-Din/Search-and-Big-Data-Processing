import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, text
from database import DatabaseConnection

from sqlalchemy import Table
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
import uuid, datetime as dt

from sqlalchemy import create_engine, text

from conf import config_112, config_201

# Database connection URL
# Format: postgresql+psycopg2://username:password@host:port/database
DATABASE_URL = "postgresql+psycopg2://postgres:Infopower_INTT55688@172.233.81.209:9300/segdb"

engine = create_engine(DATABASE_URL)

metadata = MetaData()


def transform(df, df_uif, config):

    df['lifecircle_data'] = df.nodebbUid.isin(df_uif['lifeCircleESG'])
    df['chat_sender_data'] = df.nodebbUid.isin(df_uif['chatESG_sender'])
    df['chat_receiver_data'] = df.nodebbUid.isin(df_uif['chatESG_receiver'])
    df['article_data'] = df.nodebbUid.isin(df_uif['articleAllgets'])
    
    if 'avatarImage' in df.columns:
        df.drop(columns=['avatarImage'], inplace=True)
    if 'coverImage' in df.columns:
        df.drop(columns=['coverImage'], inplace=True)
    if 'personalIntroduce' in df.columns:
        df.drop(columns=['personalIntroduce'], inplace=True)
    if 'backOfID' in df.columns:
        df.drop(columns=['backOfID'], inplace=True)
    if 'frontOfID' in df.columns:
        df.drop(columns=['frontOfID'], inplace=True)
        
    # df = df[['id', 'email', 'phone', 'nodebbUid', 'name', 'userType', 'experience', 'companyName',
    #                 'uniformNumber', 'jobTitle', 'level', 'disabled', 'createdAt', 'updatedAt', 'userKey', 
    #                 'provider', 'lifecircle_data', 'chat_sender_data', 'chat_receiver_data', 'article_data']]

    modified_df = df.rename({'phone': '電話', 'name': '名稱', 'companyName': '公司名稱', 'jobTitle': '職稱'}, axis=1)

    modified_df['belongs_to'] = config['belongs_to']

    return modified_df


def get_data():

    config = config_201
    engine = DatabaseConnection.mysql_connection(config=config, db_name='nexva')

    df_uif = {}

    with engine.connect() as connection:
        # Execute a query to get the list of tables

        for table_name in ['articleAllgets', 'lifeCircleESG', 'chatESG', 'userESG']:
            print(f"Fetching data from table: {table_name}")
        
            # Fetch data from the current table 
            connection.execute(text(f"SET NAMES utf8mb4"))

            if table_name == 'userESG':
                result = connection.execute(text(f"SELECT * FROM {table_name} where name='m7812252009'" ))
            else:
                result = connection.execute(text(f"SELECT * FROM {table_name}" ))

            # result = connection.execute(text(f"SELECT * FROM {table_name}" ))

            data = result.fetchall()
            columns = result.keys()
            # Create a DataFrame from the fetched data
            df = pd.DataFrame(data, columns=columns)

            df.dropna(axis=1,how='all', inplace=True)

            if table_name == 'articleAllgets':
                df_uif['articleAllgets'] = df.nodebbUid.unique()
            elif table_name == 'chatESG':
                df_uif['chatESG_sender'] = df.senderId.unique()
                df_uif['chatESG_receiver'] = df.senderId.unique()
            elif table_name == 'lifeCircleESG':
                df_uif['lifeCircleESG'] = df.nodebbUid.unique()
            elif table_name == 'userESG':
                df = transform(df, df_uif, config)
            
    engine.dispose()  # Close the connection when done

    return df

def _clean(value):
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (pd.Timestamp, dt.datetime, dt.date)):
        return value.isoformat()
    if pd.isna(value):
        return None

    return value

def upsert_data(table_name: str, rows: list[dict], conflict_columns: list[str] = []):
    """
    Perform UPSERT into a PostgreSQL table dynamically using reflection.
    :param table_name: Table name in the database.
    :param rows: List of dicts representing rows to insert.
    :param conflict_columns: List of columns for ON CONFLICT (must exist in the table).
    """
    if not rows:
        print(f"No data provided for {table_name}.")
        return

    # Reflect the table
    table = Table(table_name, metadata, autoload_with=engine)
    
    # for col in table.columns:
    #     print(f"Column: {col.name}, Type: {col.type}, Primary Key: {col.primary_key}")
    # Clean rows and keep only columns that really exist
    cleaned_rows = [
        {k: _clean(v) for k, v in r.items() if k in table.c}  # strip unknown cols
        for r in rows
    ]

    print(f"Upserting {len(rows)} rows into {table_name}...")
    with Session(engine) as session:
        try:
            for row in cleaned_rows:
                insert_stmt = insert(table).values(**row)
                stmt = insert_stmt.on_conflict_do_nothing(
                    index_elements=conflict_columns,
                )
                session.execute(stmt)
            session.commit()
            print(f"Upserted {len(rows)} rows into {table_name}.")
        except Exception as e:
            session.rollback()
            print(f"Error upserting into {table_name}: {e}")

def query(user_df):
    """
    Query the customer_attributes table to get the id and name of attributes.
    """
    # Extract related attribute
    with engine.connect() as connection:
        result = connection.execute(text(f'select name, id from customer_attributes where name in {tuple(user_df.columns)}'))
        return result.fetchall()