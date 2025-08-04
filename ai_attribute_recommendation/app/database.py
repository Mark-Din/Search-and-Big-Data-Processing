import json
import psycopg2
from pgvector.psycopg2 import register_vector
from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
config_json = os.path.join(os.path.dirname(__file__), "config.json")

config_json = json.load(open(config_json))


class Database:
    _instances = {}  # Store separate instances for "ai" and "origin"

    def __new__(cls, *args, **kwargs):
        raise RuntimeError("Cannot instantiate a singleton class.")

    @classmethod
    def get_instance(cls, method):
        """ Returns a singleton database connection instance for a given method. """
        print(f"Requesting database connection for _instances: {cls._instances}")
        if method not in cls._instances:
            print(f"Creating new database connection for method: {method}")
            cls._instances[method] = cls.connect(method)
        return cls._instances[method]

    @staticmethod
    def connect(method):
        """ Establishes a database connection based on the method type. """
        if method == "ai":
            dbname = config_json.get("AI_POSTGRES_DB")
            user = config_json.get("AI_POSTGRES_USER")
            password = config_json.get("AI_POSTGRES_PASSWORD")
            host = config_json.get("AI_POSTGRES_HOST")
            port = config_json.get("AI_POSTGRES_PORT")
        elif method == "origin":
            dbname = config_json.get("POSTGRES_DB")
            user = config_json.get("POSTGRES_USER")
            password = config_json.get("POSTGRES_PASSWORD")
            host = config_json.get("POSTGRES_HOST")
            port = config_json.get("POSTGRES_PORT")
        else:
            raise ValueError("Invalid database method specified.")

        print(f"Connecting to database... {dbname}, {user}, {host}, {port}")

        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )

        try:
            if method != "origin":
                register_vector(conn)  # Enable vector support for similarity search
                print(f"Database connection established for {method}.")
            return conn
        except psycopg2.ProgrammingError:
            print("pgvector extension not found.")
            return None

    @classmethod
    def close(cls, method=None):
        """ Closes the database connection(s). """
        if method:
            if method in cls._instances:
                cls._instances[method].close()
                del cls._instances[method]
                print(f"Closed {method} database connection.")
        else:
            for key in list(cls._instances.keys()):
                cls._instances[key].close()
                del cls._instances[key]
                print(f"Closed {key} database connection.")

    @staticmethod
    def init_tables(conn):
        try:
            cur = conn.cursor()

            # Users Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT UNIQUE NOT NULL,
                    user_name TEXT
                );
            """)

            # Segments Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS segments (
                    id SERIAL PRIMARY KEY,
                    segment_name varchar(20) UNIQUE NOT NULL,
                    embedding_segment vector(1024)
                );
            """)

            # Recommendations Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT REFERENCES users(user_id) ON DELETE CASCADE,
                    segment_id INTEGER REFERENCES segments(id) ON DELETE CASCADE,
                    recommend_json_ml TEXT,
                    embedding_recommend_ml vector(1024),
                    eval_score REAL,
                    created_at TIMESTAMP DEFAULT current_timestamp
                );
            """)

            # User Recommendations Table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_recommendations (
                    id char(64) PRIMARY KEY,
                    recommendation_id INTEGER REFERENCES recommendations(id) ON DELETE CASCADE,
                    recommend_json_user TEXT,
                    embedding_recommend_user vector(1024),
                    eval_score REAL,
                    accepted BOOLEAN DEFAULT FALSE
                );
            """)

            # Create Indexes for Vector Search Optimization
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_embedding_segment 
                ON segments USING ivfflat (embedding_segment vector_cosine_ops) 
                WITH (lists=50);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_embedding_recommend_ml 
                ON recommendations USING ivfflat (embedding_recommend_ml vector_cosine_ops) 
                WITH (lists=50);
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_embedding_recommend_user 
                ON user_recommendations USING ivfflat (embedding_recommend_user vector_cosine_ops) 
                WITH (lists=50);
            """)

            conn.commit()
            print("Database tables initialized successfully.")
        except Exception as e:
            print(f"Error initializing database tables: {e}")
            return False