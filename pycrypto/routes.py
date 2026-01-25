from http import HTTPStatus

from fastapi import APIRouter

from pycrypto.commons.cache import Cache
from pycrypto.commons.database import Database
from pycrypto.schemas import DefaultMessage

router = APIRouter()


@router.get("/", status_code=HTTPStatus.OK, response_model=DefaultMessage)
async def root():
    try:
        Database()
        Cache()
        message = "OK"
        error = ""
    except Exception as e:
        message = "ERROR"
        error = e

    return {"message": message, "error": error}
