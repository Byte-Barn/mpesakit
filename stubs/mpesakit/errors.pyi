from _typeshed import Incomplete
from pydantic import BaseModel
from typing import Any

class MpesaError(BaseModel):
    request_id: str | None
    error_code: str | None
    error_message: str | None
    status_code: int | None
    raw_response: dict[str, Any] | None

class MpesaApiException(Exception):
    error: Incomplete
    def __init__(self, error: MpesaError) -> None: ...
    @property
    def error_code(self) -> str | None: ...
    @property
    def request_id(self) -> str | None: ...
