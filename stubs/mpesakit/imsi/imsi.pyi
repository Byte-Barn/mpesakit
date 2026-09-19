from abc import ABC
from typing import Type
from pydantic import BaseModel

from mpesakit.auth import AsyncTokenManager, TokenManager
from mpesakit.http_client import AsyncHttpClient, HttpClient

from .schemas import (
    IMSIRequest,
    BaseIMSISelfResponse,
)

class BaseIMSIVersionStrategy(ABC):
    @property
    def endpoint_path(self) ->str:...
    @property
    def response_schema(self) ->Type[BaseIMSISelfResponse]:...

class IMSIV1Strategy(BaseIMSIVersionStrategy):...

class IMSIV2Strategy(BaseIMSIVersionStrategy):...

class IMSIV3Strategy(BaseIMSIVersionStrategy):...

class IMSI(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    version_strategy: BaseIMSIVersionStrategy = IMSIV1Strategy()
    def query(self,request: IMSIRequest) -> BaseIMSISelfResponse:...

class AsyncIMSI(BaseModel):
    http_client: AsyncHttpClient
    token_manager: AsyncTokenManager
    version_strategy: BaseIMSIVersionStrategy = IMSIV1Strategy()
    async def query(self,request: IMSIRequest) -> BaseIMSISelfResponse:...
