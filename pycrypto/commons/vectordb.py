import os

import psycopg
from dotenv import load_dotenv
from pgvector.psycopg import register_vector
from psycopg_pool import ConnectionPool

from pycrypto.commons.utils import Singleton

load_dotenv()


class Database(metaclass=Singleton):
    def __init__(self):
        db_params = tuple(
            map(os.getenv, ["PGVECTOR_USER", "PGVECTOR_PASSWORD", "PGVECTOR_HOST", "PGVECTOR_PORT", "PGVECTOR_DB"])
        )
        string_connection = "postgres://{}:{}@{}:{}/{}".format(*db_params)
        self.conn = psycopg.connect(string_connection, autocommit=True)

        self.pool = ConnectionPool(
            conninfo=string_connection, open=True, min_size=2, max_size=10, configure=self.configure
        )

    def configure(self, conn: psycopg.Connection):
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        register_vector(conn)

    def insert_embedding(self, content, embedding, metadata):
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO rag001 (content, embedding, metadata) VALUES (%s, %s, %s)",
                    (content, embedding, metadata),
                )
