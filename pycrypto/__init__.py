from dotenv import load_dotenv

from pycrypto.commons import Cache, Database, VectorDatabase

load_dotenv()
db = Database()
cache = Cache()
vdb = VectorDatabase()

__all__ = ["Cache", "Database", "VectorDatabase"]
