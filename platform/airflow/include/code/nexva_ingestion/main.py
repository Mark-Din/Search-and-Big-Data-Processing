import paramiko
import boto3
import hashlib
import os

from datetime import datetime
from typing import Any
from sqlalchemy import text

from common.config import config_41_mysql, config_pg, config_43_minio, conf_41_ssh
from common.connection import DatabaseConnection
from common.init_log import initlog

logger = initlog("ETL")

engine_mysql = DatabaseConnection.mysql_connection(config=config_41_mysql)
engine_pg = DatabaseConnection.postgres_connection(config=config_pg)

file_loc = conf_41_ssh
hostname = file_loc['ip']
port = file_loc['port']
username = file_loc['username']
password = file_loc['password']
remote_dir = file_loc['remote_dir']

minio_loc = config_43_minio
bucket = minio_loc['bucket']

# 209 remote_dir = "/opt/docker-volume/backend/dist/web_upload"

# Connect SSH
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname, port, username, password)
sftp = ssh.open_sftp()

# Connect MinIO
s3 = boto3.client(
    "s3",
    endpoint_url=f"http://{minio_loc['MINIO_ENDPOINT']}",
    aws_access_key_id=minio_loc['MINIO_ACCESS_KEY'],
    aws_secret_access_key=minio_loc['MINIO_SECRET_KEY']
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=bucket)
except:
    s3.create_bucket(Bucket=bucket)

# Chunk size for streaming (8 MB)
CHUNK_SIZE = 8 * 1024 * 1024

# Create a custom generator for chunks
def file_chunk_generator(file_obj, chunk_size):
    while True:
        data = file_obj.read(chunk_size)
        if not data:
            break
        yield data

# Use a streaming approach for MinIO upload
class StreamingBody:
    def __init__(self, generator):
        self.generator = generator
    def read(self, amt=None):
        try:
            return next(self.generator)
        except StopIteration:
            return b""

target_date = datetime(2025, 11, 7, 0, 0, 0) # Example: December 1, 2025

def file_exists(title, user_id, file_type):
    with engine_pg.connect() as connection:
        query = text("""
            SELECT 1
            FROM files
            WHERE title = :title
            AND nexva_uid = :user_id
            AND file_type = :file_type
            LIMIT 1
        """)
        result = connection.execute(query, {
                "title": title,
                "user_id": user_id,
                "file_type": file_type
        }).fetchone()

        return result is not None


def get_file_info(name, mysql_conn) -> dict:

        # 1️⃣ Get lifecycle id from MySQL
        data = mysql_conn.execute(
            text("""
            SELECT nodebbUid, lifeCircleESGId, originImageName
            FROM lifeCircleForum
            WHERE image = :image
            """),
            {"image": name}
        ).fetchone()

        return data


def hash_password(*args: Any) -> str:
    key = "_".join("" if arg is None else str(arg) for arg in args)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# def sync_users_from_mysql(mysql_conn, pg_conn):

#     insert_sql = """
#         INSERT INTO users (nexva_uid, user_name, email, hashed_password, is_active)
#         VALUES (:nexva_uid, :user_name, :email, :hashed_password, TRUE)
#         ON CONFLICT (nexva_uid) DO NOTHING
#         RETURNING id
#     """

#     select_sql = """
#         SELECT id FROM users WHERE nexva_uid = :nexva_uid
#     """

#     try:
#         inserted = 0
#         skipped = 0

#         query = text("""
#             SELECT nodebbUid, name, email
#             FROM userESG
#             WHERE nodebbUid IS NOT NULL AND updatedAt >= '2025-11-07'
#         """)

#         results = mysql_conn.execute(query)

#         pg_user_id = {}

#         for row in results:

#             result = pg_conn.execute(
#                 text(insert_sql),
#                 {
#                     "nexva_uid": row.nodebbUid,
#                     "user_name": row.name,
#                     "email": row.email,
#                     "hashed_password": hash_password(row.nodebbUid, row.email)
#                 }
#             )

#             inserted_id = result.scalar()  # ✅ returns id if inserted

#             if inserted_id:
#                 inserted += 1
#                 user_id = inserted_id
#             else:
#                 skipped += 1
#                 user_id = pg_conn.execute(
#                     text(select_sql),
#                     {"nexva_uid": row.nodebbUid}
#                 ).scalar()

#             pg_user_id[row.nodebbUid] = user_id

#             logger.info(f"Inserted: {inserted}, Skipped: {skipped}")

#         return pg_user_id

#     except Exception:
#         logger.exception("sync_users_from_mysql failed")
#         raise

def get_or_create_user(pg_conn, nexva_uid, mysql_conn):

    # 1. Fetch from Nexva/MySQL
    user_data = mysql_conn.execute(text("""
        SELECT name, email
        FROM userESG
        WHERE nodebbUid = :uid
    """), {"uid": nexva_uid}).fetchone()

    if not user_data:
        raise Exception(f"User {nexva_uid} not found in Nexva")

    result = pg_conn.execute(text("""
        WITH upsert AS (
            INSERT INTO users (nexva_uid, user_name, email, hashed_password, is_active)
            VALUES (:uid, :name, :email, :pwd, TRUE)
            ON CONFLICT (nexva_uid)
            DO UPDATE SET
                user_name = EXCLUDED.user_name
            WHERE users.user_name IS DISTINCT FROM EXCLUDED.user_name
            RETURNING id
        )
        SELECT id FROM upsert
        UNION ALL
        SELECT id FROM users WHERE nexva_uid = :uid
        LIMIT 1
    """), {
        "uid": nexva_uid,
        "name": user_data.name,
        "email": user_data.email,
        "pwd": "password_not_set"
    })

    return result.scalar()

def upload_files():

    logger.info("Start extracting files")
    
    try:
        with engine_pg.begin() as connection, engine_mysql.begin() as mysql_conn:

            # pg_user_id = sync_users_from_mysql(mysql_conn, connection)

            for file_attr in sftp.listdir_attr(remote_dir):

                if datetime.fromtimestamp(file_attr.st_mtime) < target_date:
                    continue

                file = file_attr.filename

                name, ext = os.path.splitext(file)
                file_name = name.lower()
                file_type = ext[1:].lower()

                if file_type not in ['jpg','png','pdf','mp4','mp3','docx','txt','json','xml','csv','xlsx','xls','ndjson']:
                    continue
                
                logger.info(f'file name ============= {file}')
                result = get_file_info(file, mysql_conn)
                
                if not result:
                    logger.info(f'{file} result not founnd')
                    continue

                nexva_id, life_circle_id, title = result[0], result[1], result[2]

                user_id = get_or_create_user(connection, nexva_id, mysql_conn)

                logger.info(f'result from get_file_info============= {result}')

                file_info_dict = {
                        "nexva_id": nexva_id,
                        "life_circle_id": life_circle_id,
                        "file_type": file_type,
                        "file_name": file_name
                    }
                
                logger.info(f'file_info_dict ==== {file_info_dict}')

                # determine target directory
                if file_type in ['csv','xlsx','xls']:
                    target_dir = f'structured/{user_id}/'
                elif file_type in ['json','xml','ndjson']:
                    target_dir = f'semi-structured/{user_id}/'
                else:
                    target_dir = f'unstructured/{user_id}/'

                file_path = os.path.join(target_dir, file_name)
                object_key = f"{file_path}.{file_type}"

                # 🔹 Check metadata duplication
                # if file_exists(file_name, nexva_id, file_type):
                #     logger.info(f"Skip existing metadata {file_name}")
                #     continue

                remote_path = f"{remote_dir}/{file}"

                try:
                    # Upload object
                    with sftp.open(remote_path, "rb") as remote_file:

                        s3.upload_fileobj(
                            StreamingBody(file_chunk_generator(remote_file, CHUNK_SIZE)),
                            bucket,
                            object_key
                        )

                        logger.info(f"Uploaded and stored metadata {object_key}")

                        # Insert metadata
                        file_model = connection.execute(text("""
                            INSERT INTO files (title, file_path, file_type, file_size, topic_id, owner_id, nexva_uid, life_circle_id)
                            VALUES (:title, :file_path, :file_type, :file_size, :topic_id, :owner_id, :nexva_uid, :life_circle_id)
                            ON CONFLICT (title, owner_id, file_type) DO update SET
                            file_path = EXCLUDED.file_path,
                            file_size = EXCLUDED.file_size,
                            topic_id = EXCLUDED.topic_id,
                            life_circle_id = EXCLUDED.life_circle_id
                            RETURNING id
                        """), {
                            "title": title.split('.')[0] if title is not None else file_name.split('.')[0],
                            "file_path": file_path,
                            "file_type": file_type,
                            "file_size": file_attr.st_size,
                            "topic_id": 1,  # 1 is nexva
                            "owner_id": user_id,
                            "nexva_uid": nexva_id,
                            "life_circle_id": life_circle_id
                        }).fetchone()
                    
                    logger.info(
                        f"Insert files successfully {file_model.id}"
                    )

                except Exception as e:
                    
                    logger.error(f"Failed processing {file}: {e}", exc_info=True)
                    # connection.rollback()  # Rollback transaction on error
                    # compensation for preventing file and metadata inconsistency
                    try:
                        s3.delete_object(Bucket=bucket, Key=object_key)
                        logger.info(f"Rollback: deleted object {object_key}")
                    except Exception as delete_error:
                        logger.error(f"Failed rollback deletion {delete_error}")

    except Exception as e:
        logger.error(f"Failed to upload files: {e}", exc_info=True)

def main():

    upload_files()

if __name__ == "__main__":
    main()