from .schemas import C2BRegisterUrlRequest as C2BRegisterUrlRequest, C2BRegisterUrlResponse as C2BRegisterUrlResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class C2B(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def register_url(self, request: C2BRegisterUrlRequest) -> C2BRegisterUrlResponse: ...
