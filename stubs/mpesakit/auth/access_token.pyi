from datetime import datetime
from pydantic import BaseModel, ConfigDict as ConfigDict
from typing import ClassVar

class AccessToken(BaseModel):
    token: str
    creation_datetime: datetime
    expiration_time: int
    model_config: ClassVar[ConfigDict]
    def is_expired(self) -> bool: ...
