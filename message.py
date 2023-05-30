from typing import Optional
from uuid import UUID, uuid4
from pydantic import BaseModel, Field

class Message(BaseModel):
    uuid: Optional[UUID] = Field(default_factory=uuid4)
    msg: str

    def __init__(self, msg: str, uuid: Optional[UUID] = None):
        super().__init__(msg=msg, uuid=uuid or uuid4())