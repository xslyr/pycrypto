from pydantic import BaseModel


class DefaultMessage(BaseModel):
    message: str
    error: str
