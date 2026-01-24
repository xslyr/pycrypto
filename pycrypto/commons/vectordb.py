import os

import psycopg
from dotenv import load_dotenv
from pgvector.psycopg import register_vector
from psycopg_pool import ConnectionPool

load_dotenv()


db_params = tuple(
    map(os.getenv, ["PGVECTOR_USER", "PGVECTOR_PASSWORD", "PGVECTOR_HOST", "PGVECTOR_PORT", "PGVECTOR_DB"])
)
string_connection = "postgres://{}:{}@{}:{}/{}".format(*db_params)
connetction = psycopg.connect(string_connection, autocommit=True)


def configure(conn):
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    register_vector(conn)


pool = ConnectionPool(
    conninfo=string_connection,
    open=True,
    min_size=2,
    max_size=10,
)


def insert_embedding(content, embedding, metadata):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rag001 (content, embedding, metadata) VALUES (%s, %s, %s)", (content, embedding, metadata)
            )


# Ao encerrar a aplicação
# pool.close()
