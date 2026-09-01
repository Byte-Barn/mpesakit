from typing import Literal
from pydantic import BaseModel, ConfigDict
from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient
from .schemas import SwapRequest, SwapResponse


class Swap(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager

    model_config = ConfigDict(arbitrary_types_allowed=True)
    def swap_request(self, request: SwapRequest) -> SwapResponse: ...


class AsyncSwap(BaseModel):
    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager
    model_config = ConfigDict(arbitrary_types_allowed=True)
    async def swap_request(self, request: SwapRequest) -> SwapResponse: ...
