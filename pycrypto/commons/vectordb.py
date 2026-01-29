import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from pycrypto.commons.utils import Singleton


class VectorDatabase(metaclass=Singleton):
    def __init__(self, **kwargs):
        db_params = tuple(
            map(os.getenv, ["PGVECTOR_USER", "PGVECTOR_PASSWORD", "PGVECTOR_HOST", "PGVECTOR_PORT", "PGVECTOR_DB"])
        )
        self.__connection_str = "postgresql+psycopg://{}:{}@{}:{}/{}".format(*db_params)
        self.__engine = kwargs.get(
            "mock_engine", create_engine(self.__connection_str, pool_size=5, max_overflow=10, pool_timeout=20)
        )

    @property
    def connection_str(self):
        return self.__connection_str

    @property
    def connection(self):
        with Session(self.__engine) as session:
            yield session
