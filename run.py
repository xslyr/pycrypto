import uvicorn
from fastapi import FastAPI

from pycrypto.routes import router

app = FastAPI(title="pycrypto")
app.include_router(router)

if __name__ == "__main__":
    uvicorn.run("run:app", host="127.0.0.1", port=9000, reload=True, reload_includes=["pycrypto"])
