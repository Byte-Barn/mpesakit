from .schemas import StkPushQueryRequest as StkPushQueryRequest, StkPushQueryResponse as StkPushQueryResponse, StkPushSimulateRequest as StkPushSimulateRequest, StkPushSimulateResponse as StkPushSimulateResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class StkPush(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def push(self, request: StkPushSimulateRequest) -> StkPushSimulateResponse: ...
    def query(self, request: StkPushQueryRequest) -> StkPushQueryResponse: ...
