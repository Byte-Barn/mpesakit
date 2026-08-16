from typing import ClassVar
from pydantic import BaseModel, ConfigDict


class SwapRequest(BaseModel):
    customerNumber: str
    model_config = ClassVar[ConfigDict]

    def validate_customer_number(cls, v: str) -> str: ...


class SwapResponse(BaseModel):
    responseCode: str
    requestRefID: str
    responseDesc: str
    lastSwapDate: str

    def is_successful(self) -> bool: ...
    def is_recently_swapped(self) -> bool: ...
