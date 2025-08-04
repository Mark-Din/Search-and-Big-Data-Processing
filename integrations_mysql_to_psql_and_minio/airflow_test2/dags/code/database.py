from sqlalchemy import create_engine, text
from urllib.parse import quote

from sqlalchemy import create_engine


class DatabaseConnection:
    """Handles database connections."""

    @staticmethod
    def create_connection_string(user, password, host, port, db_name=None, driver=''):
        password = str(password)
        password = quote(password)  # Ensure the password is URL encoded
        db_path = f"{host}:{port}"
        if db_name:
            db_path += f"/{db_name}"
        return f"{driver}://{user}:{password}@{db_path}"

    @staticmethod
    def mysql_connection(config, db_name=None):
        password = config['MYSQL_PASSWORD']
        driver = 'mysql+mysqlconnector'
        user = 'root'
        host = config['MYSQL_HOST']
        port = config['MYSQL_PORT']
        return create_engine(DatabaseConnection.create_connection_string(user, password, host, port, db_name, driver))

    @staticmethod
    def postgres_connection(config, db_name):
        password = config['POSTGRESQL_PASSWORD']
        driver = 'postgresql+psycopg2'
        user = 'postgres'
        host = config['POSTGRESQL_HOST']
        port = config['POSTGRESQL_PORT']
        return create_engine(DatabaseConnection.create_connection_string(user, password, host, port, db_name, driver))

def create_database(engine, db_name):
    """Creates a database if it does not exist."""
    with engine.connect() as connection:
        connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
