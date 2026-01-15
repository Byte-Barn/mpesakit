from .schemas import ReversalRequest as ReversalRequest, ReversalResponse as ReversalResponse
from _typeshed import Incomplete
from mpesakit.auth import TokenManager as TokenManager
from mpesakit.http_client import HttpClient as HttpClient
from pydantic import BaseModel

class Reversal(BaseModel):
    http_client: HttpClient
    token_manager: TokenManager
    model_config: Incomplete
    def reverse(self, request: ReversalRequest) -> ReversalResponse: ...
