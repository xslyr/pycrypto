from dotenv import load_dotenv

from pycrypto.commons.cache import Cache
from pycrypto.commons.database import Database
from pycrypto.commons.vectordb import VectorDatabase

load_dotenv(override=False)
db = Database()
cache = Cache()
vdb = VectorDatabase()

__version__ = "0.1.0"

__all__ = ["Cache", "Database", "VectorDatabase"]
